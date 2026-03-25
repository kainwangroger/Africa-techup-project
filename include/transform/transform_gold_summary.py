import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BOOKS_SILVER = BASE_DIR / "data" / "silver" / "books" / "books_clean.csv"
COUNTRIES_SILVER = BASE_DIR / "data" / "silver" / "countries" / "countries_silver.parquet"
POPULATION_RAW = BASE_DIR / "data" / "raw" / "worldbank" / "population_all.parquet"
RATES_SILVER = BASE_DIR / "data" / "silver" / "exchange_rates" / "rates_silver.parquet"
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
        logger.info("Début création table GOLD unifiée (4 sources)")
        
        # 1. Livres
        total_books = 0
        if BOOKS_SILVER.exists():
            df_books = pd.read_csv(BOOKS_SILVER)
            total_books = len(df_books)
        
        # 2. Pays
        total_countries = 0
        if COUNTRIES_SILVER.exists():
            df_countries = pd.read_parquet(COUNTRIES_SILVER)
            total_countries = len(df_countries)
            
        # 3. Population (World Bank)
        avg_population = 0
        if POPULATION_RAW.exists():
            df_pop = pd.read_parquet(POPULATION_RAW)
            # On prend la moyenne de la population de la dernière année disponible
            latest_year = df_pop["year"].max()
            avg_population = df_pop[df_pop["year"] == latest_year]["population"].mean()
            
        # 4. Taux de change (GBP/EUR)
        gbp_eur_rate = 0
        if RATES_SILVER.exists():
            df_rates = pd.read_parquet(RATES_SILVER)
            # On suppose qu'il n'y a qu'une ligne ou qu'on prend la plus récente
            if "EUR" in df_rates.columns:
                gbp_eur_rate = df_rates["EUR"].iloc[0]
        
        summary = {
            "total_books": total_books,
            "total_countries": total_countries,
            "avg_global_population": avg_population,
            "gbp_eur_rate": gbp_eur_rate,
            "processed_at": datetime.utcnow()
        }
        
        df_gold = pd.DataFrame([summary])
        
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = GOLD_DIR / "project_summary.parquet"
        df_gold.to_parquet(file_path, index=False)
        
        logger.info("✅ Table GOLD unifiée créée avec succès")
        return df_gold
        
    except Exception as e:
        logger.error(f"Erreur création GOLD unifiée : {e}")
        return None

if __name__ == "__main__":
    create_gold_summary()
