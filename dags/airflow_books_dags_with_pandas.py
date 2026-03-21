from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
from functools import partial
from pathlib import Path

# ==========================
# IMPORT PIPELINE FUNCTIONS
# ==========================
from include.extract.scrap_links import get_all_book_links, save_book_links
from include.extract.scrap_books import save_books_info_to_csv, save_books_info_to_json
from include.extract.extract_countries_api import extract_countries, save_countries_data
from include.extract.extract_worldbank_api import extract_population_all, save_population_raw
from include.extract.extract_exchange_rates import extract_exchange_rates, save_exchange_rates

from include.transform.transform_books_pandas import transform_books
from include.transform.transform_countries_api_pandas import transform_countries_pandas, save_countries_silver
from include.transform.transform_worldbank_pandas import transform_population_pandas, save_population_silver
from include.transform.transform_exchange_rates_pandas import transform_exchange_rates, save_rates_silver

from include.load.save_to_minio import (
    load_minio_config, 
    create_minio_client,
    ensure_bucket_exists,
    upload_folder
)

# --------------------------
# PATHS
# --------------------------
BASE_DIR = Path("/opt/airflow/data")
RAW_DIR = BASE_DIR / "raw"
SILVER_DIR = BASE_DIR / "silver"

LINKS_FILE = RAW_DIR / "book_links.txt"
CSV_FILES = RAW_DIR / "books_raw_unique.csv"
OUTPUT_FILES = SILVER_DIR / "books_clean.csv"

# --------------------------
# DEFAULT ARGS
# --------------------------
DEFAULT_ARGS = {
    "owner": "kainwang",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==========================
# TASK FUNCTIONS
# ==========================
def extract_books_task():
    with requests.Session() as session:
        links = get_all_book_links(session)
        save_book_links(LINKS_FILE, links)
    save_books_info_to_csv(CSV_FILES)

def extract_countries_task():
    with requests.Session() as session:
        df = extract_countries(session)
        save_countries_data(Path("/opt/airflow"), df)

def extract_worldbank_task():
    with requests.Session() as session:
        df = extract_population_all(session)
        save_population_raw(df)

def extract_rates_task():
    df = extract_exchange_rates()
    save_exchange_rates(df)

def transform_books_task():
    transform_books(str(CSV_FILES), str(OUTPUT_FILES))

def upload_to_minio_task():
    config = load_minio_config(from_docker=True)
    client = create_minio_client(config)
    ensure_bucket_exists(client, config["bucket"])
    upload_folder(client, config["bucket"], RAW_DIR, "bronze")
    upload_folder(client, config["bucket"], SILVER_DIR, "silver")

# ==========================
# DAG DEFINITION
# ==========================
with DAG(
    dag_id="books_etl_pandas_pipeline",
    description="Multi-source ETL (4 sources) using Pandas",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "multi-source", "pandas", "minio"],
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
