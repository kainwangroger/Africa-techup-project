import sys
from pathlib import Path
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# IMPORT EXTRACT
# -------------------------
from include.extract.extract_worldbank_api import main as extract_worldbank
from include.extract.extract_countries_api import main as extract_countries
from include.extract.extract_kaggle_csv import main as extract_kaggle
from include.extract.scrap_books import main as extract_books
from include.extract.extract_exchange_rates import main as extract_rates

# -------------------------
# IMPORT TRANSFORM (PANDAS)
# -------------------------
from include.transform.transform_worldbank_pandas import main as transform_population
from include.transform.transform_countries_api_pandas import main as transform_countries
from include.transform.transform_kaggle_wine_pandas import main as transform_wine
from include.transform.transform_books_pandas import main as transform_books_pandas
from include.transform.transform_exchange_rates_pandas import main as transform_rates

# -------------------------
# IMPORT GOLD
# -------------------------
from include.transform.transform_gold_summary import create_gold_summary

# -------------------------
# IMPORT LOAD (POSTGRES)
# -------------------------
from include.load.load_postgres import main as load_postgres
from include.load.load_to_postgres import load_gold_data

# -------------------------
# IMPORT LOAD (MINIO)
# -------------------------
from include.load.save_to_minio import (
    load_minio_config,
    create_minio_client,
    ensure_bucket_exists,
    upload_folder
)

# -------------------------
# PATHS
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
SILVER_DIR = BASE_DIR / "data" / "silver"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)


def run_step(step_name, func):
    try:
        logger.info(f"===== START {step_name} =====")
        func()
        logger.info(f"===== END {step_name} =====")
    except Exception as e:
        logger.error(f"Erreur dans {step_name} : {e}")
        sys.exit(1)  # Stop pipeline si erreur


def upload_to_minio():
    """Upload Bronze et Silver vers MinIO (mode local)."""
    config = load_minio_config(from_docker=False)
    client = create_minio_client(config)
    ensure_bucket_exists(client, config["bucket"])
    upload_folder(client, config["bucket"], RAW_DIR, "bronze")
    upload_folder(client, config["bucket"], SILVER_DIR, "silver")


def main():

    logger.info("🚀 PIPELINE AFRICA-TECHUP START")

    # -------------------------
    # EXTRACT
    # -------------------------
    run_step("EXTRACT WORLDBANK", extract_worldbank)
    run_step("EXTRACT COUNTRIES", extract_countries)
    run_step("EXTRACT KAGGLE", extract_kaggle)
    run_step("EXTRACT BOOKS", extract_books)
    run_step("EXTRACT EXCHANGE RATES", extract_rates)

    # -------------------------
    # TRANSFORM
    # -------------------------
    run_step("TRANSFORM POPULATION", transform_population)
    run_step("TRANSFORM COUNTRIES", transform_countries)
    run_step("TRANSFORM WINE", transform_wine)
    run_step("TRANSFORM EXCHANGE RATES", transform_rates)
    run_step("TRANSFORM BOOKS", transform_books_pandas)

    # -------------------------
    # GOLD
    # -------------------------
    run_step("TRANSFORM GOLD SUMMARY", create_gold_summary)

    # -------------------------
    # LOAD POSTGRES
    # -------------------------
    run_step("LOAD POSTGRES (POPULATION)", load_postgres)
    run_step("LOAD POSTGRES (GOLD)", load_gold_data)

    # -------------------------
    # LOAD MINIO
    # -------------------------
    run_step("UPLOAD TO MINIO", upload_to_minio)

    logger.info("✅ PIPELINE TERMINÉ AVEC SUCCÈS")


if __name__ == "__main__":
    main()