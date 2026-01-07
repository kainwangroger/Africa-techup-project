from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import math

from scripts.etl_scrap_books import transform_books
from scripts.etl_scrap_books import main as save_to_minio
from scripts.scrap_links import (get_all_book_links, save_book_links)  # noqa: E501
from scripts.etl_scrap_books import (extract_book_info, save_books_info_to_csv)  # noqa: E501


# --------------------------------------------------
# CONFIG
# --------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BOOKS_PER_TASK = 100
TOTAL_BOOKS = 1000
NB_TASKS = math.ceil(TOTAL_BOOKS / BOOKS_PER_TASK)


# --------------------------------------------------
# DAG
# --------------------------------------------------
with DAG(
    dag_id="books_etl_parallel_pipeline",
    description="ETL Books with parallel extraction",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "parallel", "books"],
) as dag:

    # --------------------------------------------------
    # START / END
    # --------------------------------------------------
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # --------------------------------------------------
    # EXTRACT LINKS
    # --------------------------------------------------
    extract_links_tasks = PythonOperator(
        task_id="extract_book_links",
        python_callable=get_all_book_links,
    )

    save_links_tasks = PythonOperator(
        task_id="save_book_links",
        python_callable=save_book_links,
    )

    extract_details_tasks = PythonOperator(
        task_id="extract_details",
        python_callable=extract_book_info,
    )

    # --------------------------------------------------
    # PARALLEL EXTRACT DETAILS
    # --------------------------------------------------
    extract_detail_tasks = []

    for i in range(NB_TASKS):
        task = PythonOperator(
            task_id=f"extract_books_details_batch_{i+1}",
            python_callable=save_books_info_to_csv,
            op_kwargs={
                "start_index": i * BOOKS_PER_TASK,
                "end_index": min((i + 1) * BOOKS_PER_TASK, TOTAL_BOOKS),
                "output_suffix": f"part_{i+1}"
            },
        )
        extract_detail_tasks.append(task)

    # --------------------------------------------------
    # TRANSFORM
    # --------------------------------------------------
    transform_tasks = PythonOperator(
        task_id="transform_books",
        python_callable=transform_books,
    )

    # --------------------------------------------------
    # LOAD MINIO
    # --------------------------------------------------
    load_minio_tasks = PythonOperator(
        task_id="upload_to_minio",
        python_callable=save_to_minio,
    )

    # --------------------------------------------------
    # DEPENDENCIES
    # --------------------------------------------------
    start >> extract_links_tasks

    extract_links_tasks >> extract_details_tasks
    extract_details_tasks >> transform_tasks >> load_minio_tasks >> end
