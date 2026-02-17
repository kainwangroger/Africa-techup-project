from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pathlib import Path
import requests

# --------------------------------------------------
# PATHS
# --------------------------------------------------
DATA_DIR = Path.home() / "airflow/data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

LINKS_FILE = RAW_DIR / "book_links.txt"
OUTPUT_CSV = RAW_DIR / "books_raw_unique.csv"
OUTPUT_JSON = RAW_DIR / "books_raw_unique.json"
CLEANED_FILE = PROCESSED_DIR / "books_clean.csv"

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
def extract_links(**context):
    from include.scripts.scrap_links import get_all_book_links, save_book_links

    """
    Scrape les liens des livres et les sauvegarde.
    XCom: nombre de liens extraits
    """
    with requests.Session() as session:
        links = get_all_book_links(session)
        save_book_links(LINKS_FILE, links)

    # XCom (métadonnée légère)
    context["ti"].xcom_push(
        key="links_count",
        value=len(links)
    )


def extract_books(**context):
    from include.scripts.scrap_books import (
        save_books_info_to_csv,
        save_books_info_to_json
    )
    """
    Lit les liens depuis le fichier et extrait les données livres.
    XCom: fichiers RAW produits
    """
    save_books_info_to_csv(OUTPUT_CSV)
    save_books_info_to_json(OUTPUT_JSON)

    raw_files = {
        "csv": str(OUTPUT_CSV),
        "json": str(OUTPUT_JSON)
    }

    context["ti"].xcom_push(
        key="raw_files",
        value=raw_files
    )


def transform_books_task(**context):
    from include.spark.transform_books import transform_books

    """
    Transforme les données avec Spark.
    XCom: fichier PROCESSED prêt
    """
    transform_books(OUTPUT_CSV, CLEANED_FILE)

    context["ti"].xcom_push(
        key="processed_file",
        value=str(CLEANED_FILE)
    )


def upload_to_minio(**context):
    from include.scripts.save_to_minio import (
        load_minio_config,
        create_minio_client,
        upload_raw_files,
        upload_processed_files,
        ensure_bucket_exists
    )
    """
    Upload RAW + PROCESSED vers MinIO
    """
    from_docker = Path("/.dockerenv").exists()

    config = load_minio_config(from_docker=from_docker)
    clients = create_minio_client(config)
    ensure_bucket_exists(clients, config["bucket"])

    upload_raw_files(clients, config["bucket"])
    upload_processed_files(clients, config["bucket"])


# --------------------------------------------------
# DAG
# --------------------------------------------------
with DAG(
    dag_id="kainwang_pipeline_etl_books",
    description="Pipeline ETL Books (Scraping → Spark → MinIO)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "books", "spark", "minio"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_extract_links = PythonOperator(
        task_id="extract_links",
        python_callable=extract_links
    )

    t_extract_books = PythonOperator(
        task_id="extract_books",
        python_callable=extract_books
    )

    t_transform = PythonOperator(
        task_id="transform_books",
        python_callable=transform_books_task
    )

    t_upload = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio
    )

    # Orchestration
    start >> t_extract_links >> t_extract_books
    t_extract_books >> t_transform >> t_upload >> end
