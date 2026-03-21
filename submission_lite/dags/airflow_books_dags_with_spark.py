from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pathlib import Path
import requests

# --------------------------------------------------
# PATHS
# --------------------------------------------------
DATA_DIR = Path("/opt/airflow/data")
RAW_DIR = DATA_DIR / "raw"
SILVER_DIR = DATA_DIR / "silver"

# --------------------------------------------------
# DEFAULT ARGS
# --------------------------------------------------
DEFAULT_ARGS = {
    "owner": "kainwang",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --------------------------------------------------
# TASK CALLABLES
# --------------------------------------------------
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

def transform_books_spark_task():
    from include.transform.spark_transform_books import transform_books_spark
    transform_books_spark()

def upload_to_minio_task():
    from include.load.save_to_minio import load_minio_config, create_minio_client, ensure_bucket_exists, upload_folder
    config = load_minio_config(from_docker=True)
    client = create_minio_client(config)
    ensure_bucket_exists(client, config["bucket"])
    upload_folder(client, config["bucket"], RAW_DIR, "bronze")
    upload_folder(client, config["bucket"], SILVER_DIR, "silver")

# --------------------------------------------------
# DAG
# --------------------------------------------------
with DAG(
    dag_id="kainwang_pipeline_etl_books",
    description="Multi-source ETL (Scraping + APIs → Spark → MinIO)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "multi-source", "spark", "minio"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # EXTRACT
    t_ext_books = PythonOperator(task_id="extract_books", python_callable=extract_books_task)
    t_ext_countries = PythonOperator(task_id="extract_countries", python_callable=extract_countries_task)
    t_ext_worldbank = PythonOperator(task_id="extract_worldbank", python_callable=extract_worldbank_task)
    t_ext_rates = PythonOperator(task_id="extract_rates", python_callable=extract_rates_task)

    # TRANSFORM (Spark only for books, Pandas for others to keep it simple or use Spark for all?)
    # To keep it "Spark oriented" as requested:
    t_tr_books = PythonOperator(task_id="transform_books_spark", python_callable=transform_books_spark_task)
    
    # Other transforms can stay Pandas or we bisa use Spark if they exist.
    # Current project has transform_countries_api_pandas.
    from include.transform.transform_countries_api_pandas import transform_countries_pandas, save_countries_silver
    from include.transform.transform_worldbank_pandas import transform_population_pandas, save_population_silver
    from include.transform.transform_exchange_rates_pandas import transform_exchange_rates, save_rates_silver

    t_tr_countries = PythonOperator(task_id="transform_countries", 
                                    python_callable=lambda: save_countries_silver(transform_countries_pandas()))
    t_tr_worldbank = PythonOperator(task_id="transform_worldbank", 
                                    python_callable=lambda: save_population_silver(transform_population_pandas()))
    t_tr_rates = PythonOperator(task_id="transform_rates", 
                                python_callable=lambda: save_rates_silver(transform_exchange_rates()))

    # LOAD
    t_load = PythonOperator(task_id="upload_to_minio", python_callable=upload_to_minio_task)

    start >> [t_ext_books, t_ext_countries, t_ext_worldbank, t_ext_rates]
    
    t_ext_rates >> t_tr_rates
    [t_ext_books, t_tr_rates] >> t_tr_books
    
    t_ext_countries >> t_tr_countries
    t_ext_worldbank >> t_tr_worldbank
    
    [t_tr_books, t_tr_countries, t_tr_worldbank, t_tr_rates] >> t_load >> end
