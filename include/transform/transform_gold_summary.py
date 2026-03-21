import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BOOKS_SILVER = BASE_DIR / "data" / "silver" / "books_clean.csv"
COUNTRIES_SILVER = BASE_DIR / "data" / "silver" / "countries" / "countries_silver.parquet"
GOLD_DIR = BASE_DIR / "data" / "gold"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# TRANSFORM GOLD
# -------------------------
def create_gold_summary() -> pd.DataFrame:
    try:
        logger.info("Début création table GOLD (Summary)")
        
        if not BOOKS_SILVER.exists() or not COUNTRIES_SILVER.exists():
            logger.warning("Fichiers Silver manquants")
            return None
            
        df_books = pd.read_csv(BOOKS_SILVER)
        df_countries = pd.read_parquet(COUNTRIES_SILVER)
        
        # Exemple d'agrégation : Prix moyen des livres par pays (si on avait l'info pays dans les livres)
        # Mais ici on va faire une table de synthèse simple :
        # Top 5 pays par population + Stats globales livres
        
        summary = {
            "total_books": len(df_books),
            "avg_price_gbp": df_books["price_incl_tax"].mean(),
            "max_stock": df_books["stock"].max(),
            "processed_at": datetime.utcnow()
        }
        
        df_gold = pd.DataFrame([summary])
        
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = GOLD_DIR / "project_summary.parquet"
        df_gold.to_parquet(file_path, index=False)
        
        logger.info("Table GOLD créée avec succès")
        return df_gold
        
    except Exception as e:
        logger.error(f"Erreur création GOLD : {e}")
        return None

if __name__ == "__main__":
    create_gold_summary()
