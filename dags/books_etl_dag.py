from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests

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
def extract_links_task(session_args=None, get_links_func=None, save_links_func=None, **context):
    """
    Scrape les liens avec une session requests locale.
    Args via op_kwargs:
        session_args: dict avec paramètres pour requests.Session()
        get_links_func: fonction pour récupérer les liens
        save_links_func: fonction pour sauvegarder les liens
    """
    session_args = session_args or {}
    with requests.Session() as session:
        # Appliquer des paramètres éventuels
        for k, v in session_args.items():
            setattr(session, k, v)

        links = get_links_func(session)
        save_links_func(links)

    context["ti"].xcom_push("links_count", len(links))


def save_books_task(save_books_func=None, **context):
    """
    Sauvegarde les livres en CSV/JSON
    """
    output_file = save_books_func()
    context["ti"].xcom_push("raw_file", output_file)


def transform_books_task(transform_func=None, input_file=None, output_file=None, **context):
    """
    Transforme les données via PySpark
    """
    transform_func(input_file, output_file)
    context["ti"].xcom_push("processed_file", output_file)


def upload_to_minio_task(
    load_minio_config=None,
    create_minio_client=None,
    ensure_bucket=None,
    upload_func=None,
    bucket_name=None,
    **context
):
    """
    Upload RAW + PROCESSED vers MinIO
    """
    config = load_minio_config()
    client = create_minio_client(config)
    ensure_bucket(client, bucket_name)
    upload_func(client, bucket_name)
    context["ti"].xcom_push("minio_upload", True)


# ==========================
# DAG DEFINITION
# ==========================
dag = DAG(
    dag_id="books_etl_param_pipeline",
    description="ETL Books fully parameterized, session inside task",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "books", "param", "minio"],
)

# ==========================
# TASKS
# ==========================
start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

t_extract_links = PythonOperator(
    task_id="extract_links",
    python_callable=extract_links_task,
    dag=dag,
    op_kwargs={
        "session_args": {"timeout": 10},  # ex. timeout pour requests
        "get_links_func": None,           # à passer via args DAG
        "save_links_func": None,          # à passer via args DAG
    }
)

t_save_books = PythonOperator(
    task_id="save_books",
    python_callable=save_books_task,
    dag=dag,
    op_kwargs={
        "save_books_func": None,  # fonction pour créer CSV/JSON
    }
)

t_transform_books = PythonOperator(
    task_id="transform_books",
    python_callable=transform_books_task,
    dag=dag,
    op_kwargs={
        "transform_func": None,   # fonction PySpark
        "input_file": None,
        "output_file": None,
    }
)

t_upload_minio = PythonOperator(
    task_id="upload_to_minio",
    python_callable=upload_to_minio_task,
    dag=dag,
    op_kwargs={
        "load_minio_config": None,
        "create_minio_client": None,
        "ensure_bucket": None,
        "upload_func": None,
        "bucket_name": None,
    }
)

# ==========================
# ORCHESTRATION
# ==========================
start >> t_extract_links >> t_save_books >> t_transform_books >> t_upload_minio >> end
