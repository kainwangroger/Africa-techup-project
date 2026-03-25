import pandas as pd
import re
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BOOKS_PROCESSED = BASE_DIR / "data" / "silver" / "books_clean.csv"
COUNTRIES_SILVER = BASE_DIR / "data" / "silver" / "countries" / "countries_silver.parquet"
RATES_SILVER = BASE_DIR / "data" / "silver" / "exchange_rates" / "rates_silver.parquet"
GOLD_DIR = BASE_DIR / "data" / "gold"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

def extract_category(url):
    try:
        if pd.isna(url): return "Unknown"
        match = re.search(r'category/books/([^/]+)_', url)
        return match.group(1).replace('-', ' ').title() if match else "Books"
    except:
        return "Books"

# -------------------------
# TRANSFORM MARKET INTELLIGENCE
# -------------------------
def create_gold_market_intelligence():
    try:
        logger.info("Début création table GOLD Market Intelligence (V4 - Fixed Columns)")
        
        # 1. Chargement des sources
        df_books = pd.read_csv(BOOKS_PROCESSED)
        df_countries = pd.read_parquet(COUNTRIES_SILVER)
        df_rates = pd.read_parquet(RATES_SILVER)
        
        # 2. Préparer les livres (sample de 50)
        df_books_sample = df_books.head(50).copy()
        
        # 3. Préparer les pays : extraire la première devise
        df_countries['main_currency'] = df_countries['currency_codes'].str.split(',').str[0].str.strip()
        
        supported_currencies = ['EUR', 'USD', 'ZAR', 'NGN', 'KES', 'EGP']
        df_countries_filtered = df_countries[df_countries['main_currency'].isin(supported_currencies)].copy()
        
        # 4. Unpivot des taux (les devises sont directement en colonnes, pas de colonne 'base')
        rate_cols = [c for c in supported_currencies if c in df_rates.columns]
        df_rates_melted = df_rates[rate_cols].melt(var_name='currency_code', value_name='rate')
        
        # 5. Joindre Pays et Taux
        df_market_base = pd.merge(df_countries_filtered, df_rates_melted, left_on='main_currency', right_on='currency_code')
        
        # 6. Cross Join avec les Livres
        df_market_base['key'] = 1
        df_books_sample['key'] = 1
        df_final = pd.merge(df_market_base, df_books_sample, on='key').drop('key', axis=1)
        
        # 7. Calculer le prix local
        df_final['price_local'] = df_final['price_gbp'] * df_final['rate']
        
        # 8. Renommer et filtrer les colonnes
        df_final = df_final.rename(columns={'name': 'country', 'titre': 'book_title'})
        cols = ['country', 'region', 'population', 'main_currency', 'rate', 'book_title', 'category', 'price_gbp', 'price_local']
        df_final = df_final[cols]
        
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = GOLD_DIR / "market_intelligence.parquet"
        df_final.to_parquet(file_path, index=False)
        
        logger.info(f"✅ Table GOLD Market Intelligence créée : {len(df_final)} lignes")
        return df_final
        
    except Exception as e:
        logger.error(f"Erreur création GOLD Market V4 : {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    create_gold_market_intelligence()
