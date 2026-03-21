import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
from pathlib import Path
from datetime import datetime
from typing import Optional
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent

SILVER_PATH = BASE_DIR / "data" / "silver" / "worldbank"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "africa_techup",
    "user": "postgres",
    "password": "postgres"
}

# Nom de la table avec le schéma
TABLE_NAME = "analytics.population_silver"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# GET LATEST FILE
# -------------------------
def get_latest_parquet(folder: Path) -> Optional[Path]:
    files = list(folder.glob("*.parquet"))
    if not files:
        return None
    return max(files, key=lambda x: x.stat().st_mtime)


# -------------------------
# CREATE TABLE IF NOT EXISTS
# -------------------------
def create_table_if_not_exists(conn):

    try:
        create_table_query = f"""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            indicator_id VARCHAR(50),
            country_iso2 VARCHAR(10),
            country_name VARCHAR(150),
            year INT,
            population DOUBLE PRECISION,
            ingestion_timestamp TIMESTAMP,
            year_month VARCHAR(10),
            PRIMARY KEY (country_iso2, year)
        );
        """

        create_index_query = f"""
        CREATE INDEX IF NOT EXISTS idx_population_country
        ON {TABLE_NAME} (country_iso2);
        """

        with conn.cursor() as cur:
            cur.execute(create_table_query)
            cur.execute(create_index_query)
            conn.commit()

        logger.info("Table PostgreSQL prête")

    except Exception as e:
        logger.error(f"Erreur création table : {e}")
        conn.rollback()


# -------------------------
# UPSERT DATA
# -------------------------
def upsert_data(conn, df: pd.DataFrame):

    try:
        columns = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]

        insert_query = f"""
        INSERT INTO {TABLE_NAME} ({','.join(columns)})
        VALUES %s
        ON CONFLICT (country_iso2, year)
        DO UPDATE SET
            population = EXCLUDED.population,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp;
        """

        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
            conn.commit()

        logger.info(f"{len(df)} lignes upsertées")

    except Exception as e:
        logger.error(f"Erreur UPSERT : {e}")
        conn.rollback()


# -------------------------
# AUDIT TABLE
# -------------------------
def audit_load(conn, rows_loaded: int, file_path: str):

    try:
        audit_query = """
        CREATE TABLE IF NOT EXISTS audit_postgres (
            table_name VARCHAR(100),
            rows_loaded INT,
            file_path TEXT,
            loaded_at TIMESTAMP
        );
        """

        insert_audit = """
        INSERT INTO audit_postgres (table_name, rows_loaded, file_path, loaded_at)
        VALUES (%s, %s, %s, %s);
        """

        with conn.cursor() as cur:
            cur.execute(audit_query)
            cur.execute(insert_audit, (
                TABLE_NAME,
                rows_loaded,
                file_path,
                datetime.utcnow()
            ))
            conn.commit()

        logger.info("Audit PostgreSQL enregistré")

    except Exception as e:
        logger.error(f"Erreur audit : {e}")
        conn.rollback()


# -------------------------
# MAIN LOAD PROCESS
# -------------------------
def main():

    try:
        logger.info("Début chargement PostgreSQL")

        latest_file = get_latest_parquet(SILVER_PATH)

        if not latest_file:
            logger.warning("Aucun fichier silver trouvé")
            return

        df = pd.read_parquet(latest_file)

        conn = psycopg2.connect(**DB_CONFIG)

        create_table_if_not_exists(conn)
        upsert_data(conn, df)
        audit_load(conn, len(df), str(latest_file))

        conn.close()

        logger.info("Chargement PostgreSQL terminé")

    except Exception as e:
        logger.error(f"Erreur globale load postgres : {e}")


if __name__ == "__main__":
    main()