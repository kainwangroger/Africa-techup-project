import pandas as pd
import psycopg2
import boto3
import io
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime
from typing import Optional
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# LOCAL
LOCAL_SILVER_PATH = BASE_DIR / "data" / "silver" / "worldbank"

# MINIO
MINIO_CONFIG = {
    "endpoint_url": "http://localhost:9000",
    "aws_access_key_id": "minioadmin",
    "aws_secret_access_key": "minioadmin",
}
MINIO_BUCKET = "silver"
MINIO_PREFIX = "worldbank/"

# POSTGRES
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

# =====================================================
# LOCAL FILE READER
# =====================================================
def get_latest_local_parquet(folder: Path) -> Optional[Path]:
    files = list(folder.glob("*.parquet"))
    if not files:
        return None
    return max(files, key=lambda x: x.stat().st_mtime)


# =====================================================
# MINIO READER
# =====================================================
def get_latest_minio_parquet():

    try:
        s3 = boto3.client("s3", **MINIO_CONFIG)

        objects = s3.list_objects_v2(
            Bucket=MINIO_BUCKET,
            Prefix=MINIO_PREFIX
        )

        if "Contents" not in objects:
            return None

        latest_object = max(
            objects["Contents"],
            key=lambda x: x["LastModified"]
        )

        obj = s3.get_object(
            Bucket=MINIO_BUCKET,
            Key=latest_object["Key"]
        )

        df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        logger.info(f"Fichier MinIO chargé : {latest_object['Key']}")

        return df, latest_object["Key"]

    except Exception as e:
        logger.error(f"Erreur lecture MinIO : {e}")
        return None, None


# =====================================================
# CREATE TABLE
# =====================================================
def create_table_if_not_exists(conn):

    create_query = f"""
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

    index_query = f"""
    CREATE INDEX IF NOT EXISTS idx_country
    ON {TABLE_NAME} (country_iso2);
    """

    with conn.cursor() as cur:
        cur.execute(create_query)
        cur.execute(index_query)
        conn.commit()

    logger.info("Table PostgreSQL prête")


# =====================================================
# UPSERT
# =====================================================
def upsert_data(conn, df: pd.DataFrame):

    columns = list(df.columns)
    values = [tuple(x) for x in df.to_numpy()]

    query = f"""
    INSERT INTO {TABLE_NAME} ({','.join(columns)})
    VALUES %s
    ON CONFLICT (country_iso2, year)
    DO UPDATE SET
        population = EXCLUDED.population,
        ingestion_timestamp = EXCLUDED.ingestion_timestamp;
    """

    with conn.cursor() as cur:
        execute_values(cur, query, values)
        conn.commit()

    logger.info(f"{len(df)} lignes upsertées")


# =====================================================
# AUDIT
# =====================================================
def audit_load(conn, rows_loaded: int, source: str, file_path: str):

    create_audit = """
    CREATE TABLE IF NOT EXISTS audit_postgres (
        table_name VARCHAR(100),
        rows_loaded INT,
        source VARCHAR(20),
        file_path TEXT,
        loaded_at TIMESTAMP
    );
    """

    insert_audit = """
    INSERT INTO audit_postgres
    (table_name, rows_loaded, source, file_path, loaded_at)
    VALUES (%s, %s, %s, %s, %s);
    """

    with conn.cursor() as cur:
        cur.execute(create_audit)
        cur.execute(insert_audit, (
            TABLE_NAME,
            rows_loaded,
            source,
            file_path,
            datetime.utcnow()
        ))
        conn.commit()

    logger.info("Audit PostgreSQL enregistré")


# =====================================================
# MAIN
# =====================================================
def main(source: str = "local"):

    try:
        logger.info(f"Chargement PostgreSQL depuis {source}")

        if source == "local":

            latest_file = get_latest_local_parquet(LOCAL_SILVER_PATH)
            if not latest_file:
                logger.warning("Aucun fichier local trouvé")
                return

            df = pd.read_parquet(latest_file)
            file_path = str(latest_file)

        elif source == "minio":

            df, key = get_latest_minio_parquet()
            if df is None:
                logger.warning("Aucun fichier MinIO trouvé")
                return

            file_path = key

        else:
            logger.error("Source invalide. Utilise 'local' ou 'minio'")
            return

        conn = psycopg2.connect(**DB_CONFIG)

        create_table_if_not_exists(conn)
        upsert_data(conn, df)
        audit_load(conn, len(df), source, file_path)

        conn.close()

        logger.info("Chargement terminé")

    except Exception as e:
        logger.error(f"Erreur globale load_postgres : {e}")


if __name__ == "__main__":
    # changer ici
    main(source="local")
    # main(source="minio")
    