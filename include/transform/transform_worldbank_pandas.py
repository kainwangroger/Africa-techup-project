import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_PATH = BASE_DIR / "data" / "raw" / "worldbank" / "population_all.parquet"
SILVER_DIR = BASE_DIR / "data" / "silver" / "worldbank"
AUDIT_DIR = BASE_DIR / "data" / "audit"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# TRANSFORM
# -------------------------
def transform_population_pandas() -> Optional[pd.DataFrame]:

    try:
        logger.info("Début transformation population (Pandas)")

        df = pd.read_parquet(RAW_PATH)

        # Nettoyage
        df = df.drop_duplicates()
        df = df[df["population"] > 0]

        df["year"] = df["year"].astype(int)
        df["population"] = df["population"].astype(float)

        # Enrichissement
        df["ingestion_timestamp"] = datetime.utcnow()
        df["year_month"] = df["year"].astype(str) + "-01"

        logger.info(f"{len(df)} lignes après transformation")

        return df

    except Exception as e:
        logger.error(f"Erreur transformation pandas : {e}")
        return None


# -------------------------
# SAVE + VERSIONING + AUDIT
# -------------------------
def save_population_silver(df: pd.DataFrame):

    try:
        if df is None or df.empty:
            logger.warning("Aucune donnée à sauvegarder")
            return

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)

        file_path = SILVER_DIR / f"population_silver_{timestamp}.parquet"
        df.to_parquet(file_path, index=False, compression="snappy")

        # Audit
        audit_df = pd.DataFrame([{
            "table_name": "population_silver",
            "rows": len(df),
            "created_at": datetime.utcnow(),
            "file_path": str(file_path)
        }])

        audit_path = AUDIT_DIR / "audit_population.parquet"

        if audit_path.exists():
            existing = pd.read_parquet(audit_path)
            audit_df = pd.concat([existing, audit_df])

        audit_df.to_parquet(audit_path, index=False)

        logger.info(f"Données silver sauvegardées : {file_path}")

    except Exception as e:
        logger.error(f"Erreur sauvegarde silver : {e}")


# -------------------------
# MAIN
# -------------------------
def main():
    df = transform_population_pandas()
    save_population_silver(df)
    logger.info("Transformation Pandas terminée")


if __name__ == "__main__":
    main()