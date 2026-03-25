from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pathlib import Path

# ------------------------------------------------------------------
# CONFIGURATION & PATHS
# ------------------------------------------------------------------
DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------------------------------------------------
# EXTRACT TASKS
# ------------------------------------------------------------------
def extract_books_task():
    from include.extract.scrap_links import create_session, get_all_book_links, save_book_links
    from include.extract.scrap_books import save_books_info_to_csv
    session = create_session()
    links = get_all_book_links(session)
    save_book_links(RAW_DIR / "book_links.txt", links)
    save_books_info_to_csv(RAW_DIR / "books_raw_unique.csv")

def extract_countries_task():
    from include.extract.extract_countries_api import create_session, extract_countries, save_countries_data
    session = create_session()
    df = extract_countries(session)
    save_countries_data(Path("/opt/airflow"), df)

def extract_worldbank_task():
    from include.extract.extract_worldbank_api import create_session, extract_population_all, save_population_raw
    session = create_session()
    df = extract_population_all(session)
    save_population_raw(df)

def extract_rates_task():
    from include.extract.extract_exchange_rates import extract_exchange_rates, save_exchange_rates
    df = extract_exchange_rates()
    save_exchange_rates(df)

# ------------------------------------------------------------------
# TRANSFORM TASKS
# ------------------------------------------------------------------
def transform_books_task():
    from include.transform.transform_books_pandas import transform_books
    transform_books(str(RAW_DIR / "books_raw_unique.csv"), str(SILVER_DIR / "books_clean.csv"))

def transform_countries_task():
    from include.transform.transform_countries_api_pandas import transform_countries_pandas, save_countries_silver
    df = transform_countries_pandas()
    save_countries_silver(df)

def transform_worldbank_task():
    from include.transform.transform_worldbank_pandas import transform_population_pandas, save_population_silver
    df = transform_population_pandas()
    save_population_silver(df)

def transform_rates_task():
    from include.transform.transform_exchange_rates_pandas import transform_exchange_rates, save_rates_silver
    df = transform_exchange_rates()
    save_rates_silver(df)

# ------------------------------------------------------------------
# TASK WRAPPERS (GOLD & SQL)
# ------------------------------------------------------------------
def transform_gold_task():
    from include.transform.transform_gold_summary import create_gold_summary
    create_gold_summary()

def load_to_postgres_task():
    from include.load.load_to_postgres import load_gold_data
    load_gold_data()

# ------------------------------------------------------------------
# LOAD TASK
# ------------------------------------------------------------------
def upload_all_to_minio_task():
    from include.load.save_to_minio import load_minio_config, create_minio_client, ensure_bucket_exists, upload_folder
    config = load_minio_config(from_docker=True)
    client = create_minio_client(config)
    ensure_bucket_exists(client, config["bucket"])
    upload_folder(client, config["bucket"], RAW_DIR, "bronze")
    upload_folder(client, config["bucket"], SILVER_DIR, "silver")

# ------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------
with DAG(
    dag_id="africa_techup_unified_pipeline",
    description="Unified ETL Pipeline (4 Sources: Books, Countries, WorldBank, Rates) → MinIO",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 */4 * * *",
    catchup=False,
    tags=["industrial", "4-sources", "pandas", "minio"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # EXTRACT
    t_ext_books = PythonOperator(task_id="extract_books", python_callable=extract_books_task)
    t_ext_countries = PythonOperator(task_id="extract_countries", python_callable=extract_countries_task)
    t_ext_worldbank = PythonOperator(task_id="extract_worldbank", python_callable=extract_worldbank_task)
    t_ext_rates = PythonOperator(task_id="extract_rates", python_callable=extract_rates_task)

    # TRANSFORM
    t_tr_books = PythonOperator(task_id="transform_books", python_callable=transform_books_task)
    t_tr_countries = PythonOperator(task_id="transform_countries", python_callable=transform_countries_task)
    t_tr_worldbank = PythonOperator(task_id="transform_worldbank", python_callable=transform_worldbank_task)
    t_tr_rates = PythonOperator(task_id="transform_rates", python_callable=transform_rates_task)

    # GOLD & SQL
    t_gold_summary = PythonOperator(task_id="transform_gold_summary", python_callable=transform_gold_task)
    t_load_sql = PythonOperator(task_id="load_to_postgres", python_callable=load_to_postgres_task)

    # LOAD (MinIO)
    t_load_minio = PythonOperator(task_id="upload_to_minio", python_callable=upload_all_to_minio_task)

    start >> [t_ext_books, t_ext_countries, t_ext_worldbank, t_ext_rates]
    
    t_ext_rates >> t_tr_rates
    [t_ext_books, t_tr_rates] >> t_tr_books
    
    t_ext_countries >> t_tr_countries
    t_ext_worldbank >> t_tr_worldbank
    
    # Gold layer depends on silver data being ready
    [t_tr_books, t_tr_countries] >> t_gold_summary >> t_load_sql
    
    # Final load to MinIO
    [t_tr_books, t_tr_countries, t_tr_worldbank, t_tr_rates, t_load_sql] >> t_load_minio >> end
