import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)

RAW_PATH = Path(__file__).resolve().parent.parent.parent / "data/raw/countries/data_countries.parquet"
SILVER_PATH = Path(__file__).resolve().parent.parent.parent / "data/silver/countries/countries_silver.parquet"

def transform_countries_pandas():
    try:
        logger.info("Début transformation Countries (Pandas)")
        df = pd.read_parquet(RAW_PATH)

        # Nettoyage
        df = df.drop_duplicates(subset=["iso3"])
        df = df[df["iso3"].notna()]
        df["population"] = pd.to_numeric(df["population"], errors="coerce")
        df["area"] = pd.to_numeric(df["area"], errors="coerce")
        df["processed_at"] = datetime.utcnow()
        df["population_density"] = df["population"] / df["area"]

        logger.info(f"{len(df)} lignes après transformation")
        return df
    except Exception as e:
        logger.error(f"Erreur transformation Countries : {e}")
        return None

def save_countries_silver(df: pd.DataFrame):
    if df is None or df.empty:
        logger.warning("Aucune donnée à sauvegarder")
        return
    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_PATH, index=False, compression="snappy")
    logger.info(f"✅ Countries silver sauvegardé : {SILVER_PATH}")

def main():
    df = transform_countries_pandas()
    save_countries_silver(df)
    logger.info("Transformation Countries terminée")

if __name__ == "__main__":
    main()