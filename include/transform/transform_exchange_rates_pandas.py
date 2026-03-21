import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_PATH = BASE_DIR / "data" / "raw" / "exchange_rates" / "rates_gbp.parquet"
SILVER_DIR = BASE_DIR / "data" / "silver" / "exchange_rates"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# TRANSFORM
# -------------------------
def transform_exchange_rates() -> pd.DataFrame:
    try:
        logger.info("Début transformation Exchange Rates")
        df = pd.read_parquet(RAW_PATH)
        
        # On pourrait ajouter des calculs ou du formatage si besoin
        df["processed_at"] = datetime.utcnow()
        
        logger.info("Transformation terminée")
        return df
    except Exception as e:
        logger.error(f"Erreur transformation Exchange Rates : {e}")
        return None

# -------------------------
# SAVE
# -------------------------
def save_rates_silver(df: pd.DataFrame):
    if df is None or df.empty:
        return
        
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    file_path = SILVER_DIR / "rates_silver.parquet"
    df.to_parquet(file_path, index=False, compression="snappy")
    logger.info(f"✅ Taux silver sauvegardés : {file_path}")

# -------------------------
# MAIN
# -------------------------
def main():
    df = transform_exchange_rates()
    save_rates_silver(df)

if __name__ == "__main__":
    main()
