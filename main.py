from include.scripts.scrap_links import (
    create_session,
    get_all_book_links,
    save_book_links
)
from include.scripts.scrap_books import (
    save_books_info_to_csv,
    save_books_info_to_json
)
from pathlib import Path
from include.utils.custom_logging import setup_logging, get_logger

from include.scripts.save_to_minio import (
    load_minio_config,
    create_minio_client,
    upload_processed_files,
    upload_raw_files
)

from include.spark.transform_books import transform_books
import requests

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# CONSTANTS
# ------------------------

BATCH_SIZE = 100
TOTAL_BOOKS = 1000

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LINKS_FILE = DATA_DIR / "book_links.txt"
OUTPUT_CSV = DATA_DIR / "books_raw_unique.csv"
OUTPUT_JSON = DATA_DIR / "books_raw_unique.json"

OUTPUT_FILES = BASE_DIR / "data" / "processed" / "books_clean.csv"

# -------------------------
# SESSION
# -------------------------
session = create_session()


# --------------------------
# MINIO CONFIG
# --------------------------
"""
    Charge la configuration MinIO depuis les variables d'environnement.
    Détecte si le script tourne dans Docker ou non.
"""
from_docker = False
if Path("/.dockerenv").exists():
    from_docker = True

config = load_minio_config(from_docker=from_docker)
clients = create_minio_client(config)
# bucket = ensure_bucket_exists(
#     clients,
#     config["bucket"]
# )


# -------------------------
# MAIN
# ------------------------
def main():
    logger.info("Démarrage du pipeline ETL")

    with requests.Session() as session:

        # 1️⃣ Extraction des liens
        logger.info("Extraction des liens")
        links = get_all_book_links(session)
        logger.info(f"{len(links)} liens récupérés")

        # 2️⃣ Sauvegarde des liens
        logger.info("Sauvegarde des liens extraits")
        save_book_links(LINKS_FILE, links)

        # 3️⃣ Extraction + sauvegarde CSV
        logger.info("Sauvegarde des informations extraites en CSV")
        save_books_info_to_csv(OUTPUT_CSV)

        # 4️⃣ Extraction + sauvegarde JSON
        logger.info("Sauvegarde des informations extraites en JSON")
        save_books_info_to_json(OUTPUT_JSON)

        # 5 Sauvegarde des données traiter avec Pyspark
        logger.info("Sauvegarde des données transformées avec Pyspark")
        transform_books(OUTPUT_CSV, OUTPUT_FILES)

    # 5️⃣ Upload MinIO
    logger.info("Configuration de MinIO")

    logger.info("Upload des fichiers RAW vers MinIO")
    upload_raw_files(clients, config["bucket"])

    logger.info("Upload des fichiers PROCESSED vers MinIO")
    upload_processed_files(clients, config["bucket"])

    logger.info("✅ Scraping terminé avec succès")


if __name__ == "__main__":
    main()
