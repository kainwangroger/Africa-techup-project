import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from typing import Optional
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG DB
# -------------------------
DB_URL = "postgresql://postgres:password@localhost:5432/ma_base"
TABLE_NAME = "ma_table"

BASE_DIR = Path(__file__).resolve().parent.parent.parent

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# EXTRACTION
# -------------------------
def extract_postgres_table() -> Optional[pd.DataFrame]:

    logger.info(f"Début extraction table {TABLE_NAME}")

    engine = create_engine(DB_URL)

    query = f"SELECT * FROM {TABLE_NAME}"

    df = pd.read_sql(query, engine)

    logger.info(f"{len(df)} lignes extraites depuis PostgreSQL")

    return df


# -------------------------
# SAUVEGARDE
# -------------------------
def save_postgres_data(base_dir: Path, df: pd.DataFrame) -> None:

    if df is None or df.empty:
        logger.warning("Aucune donnée à sauvegarder")
        return

    output_dir = base_dir / "data" / "raw" / "database"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / f"{TABLE_NAME}.parquet"

    df.to_parquet(file_path, index=False, compression="snappy")

    logger.info(f"Données sauvegardées dans {file_path}")


# -------------------------
# MAIN
# -------------------------
def main():
    df = extract_postgres_table()
    save_postgres_data(BASE_DIR, df)
    logger.info("Extraction PostgreSQL terminée")


if __name__ == "__main__":
    main()