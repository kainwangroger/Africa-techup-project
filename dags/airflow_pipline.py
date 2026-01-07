from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.scrap_links import (get_all_books_links, save_book_links)  # noqa: E501
from scripts.etl_scrap_books import (extract_book_info, save_books_info_to_csv)  # noqa: E501
# from scripts.transform_spark import transform_csv_to_parquet

RAW_TXT = "data/raw/book_links.txt"
RAW_CSV = "data/raw/books_raw.csv"
PARQUET = "data/processed/books.parquet"


# # Découper un fichier texte en plusieurs sous-fichiers
# def split_file(input_file, lines_per_file=100):
#     with open(input_file, "r", encoding="utf-8") as f:
#         lines = f.readlines()

#     # Nombre total de fichiers à créer
#     total_files = len(lines) // lines_per_file

#     for i in range(total_files):
#         # Déterminer les lignes à écrire
#         start = i * lines_per_file
#         end = start + lines_per_file
#         chunk = lines[start:end]

#         # Nom du fichier de sortie
#         output_file = f"liens_{i+1}.txt"
#         with open(output_file, "w", encoding="utf-8") as out:
#             out.writelines(chunk)

#         print(f"✅ Fichier créé : {output_file} ({len(chunk)} lignes)")


# # Exemple d'utilisation
# split_file("liens.txt", lines_per_file=100)

default_args = {
    'owner': 'Kainwang',
    'depend_on_post': False,
    'reties': 1,
    'retries_daily': timedelta(minutes=5)
}

with DAG(
    dag_id="books_project_africatechup_pipeline",
    start_date=datetime(2025, 12, 1),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["scraping", "etl", "books"]
) as dag:

    scrape_links = PythonOperator(
        task_id="scrape_links",
        python_callable=get_all_books_links,
    )

    save_links = PythonOperator(
        task_id="save_links",
        python_callable=save_book_links,
        op_args=[RAW_TXT]
    )

    scrape_books = PythonOperator(
        task_id="scrape_books",
        python_callable=extract_book_info,
    )

    save_books = PythonOperator(
        task_id="save_books",
        python_callable=save_books_info_to_csv,
        op_args=[RAW_CSV]
    )

    # transform = PythonOperator(
    #     task_id="spark_transform",
    #     python_callable=transform_csv_to_parquet,
    #     op_args=[RAW_CSV, PARQUET]
    # )

    scrape_links >> save_links >> scrape_books >> save_books
