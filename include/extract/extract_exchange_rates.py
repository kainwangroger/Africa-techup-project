import requests
import pandas as pd
from pathlib import Path
from typing import Optional
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_URL = "https://api.exchangerate-api.com/v4/latest/GBP"
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "exchange_rates"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# EXTRACTION
# -------------------------
def extract_exchange_rates() -> Optional[pd.DataFrame]:
    logger.info("Début extraction Exchange Rates (GBP base)")
    
    try:
        response = requests.get(BASE_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        rates = data.get("rates", {})
        if not rates:
            logger.warning("Aucun taux trouvé dans la réponse")
            return None
            
        # On ne garde que quelques taux pertinents
        target_currencies = ["USD", "EUR", "ZAR", "NGN", "KES", "EGP"]
        filtered_rates = {k: v for k, v in rates.items() if k in target_currencies}
        
        df = pd.DataFrame([filtered_rates])
        df["base"] = "GBP"
        df["date"] = data.get("date")
        
        logger.info(f"Taux de change extraits pour {len(filtered_rates)} devises")
        return df

    except Exception as e:
        logger.error(f"Erreur extraction Exchange Rates : {e}")
        return None

# -------------------------
# SAUVEGARDE
# -------------------------
def save_exchange_rates(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        logger.warning("Aucune donnée à sauvegarder")
        return

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    file_path = RAW_DIR / "rates_gbp.parquet"
    
    df.to_parquet(file_path, index=False, compression="snappy")
    logger.info(f"Données RAW sauvegardées dans {file_path}")

# -------------------------
# MAIN
# -------------------------
def main():
    df = extract_exchange_rates()
    save_exchange_rates(df)

if __name__ == "__main__":
    main()
