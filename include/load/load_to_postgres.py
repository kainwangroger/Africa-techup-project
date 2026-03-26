import pandas as pd
import os
from sqlalchemy import create_engine
from pathlib import Path
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
setup_logging()
logger = get_logger(__name__)

def get_engine():
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    db = os.getenv("POSTGRES_DB", "airflow")
    host = "postgres" # Utilise le nom du service docker
    port = "5432"
    
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

def load_to_postgres(df: pd.DataFrame, table_name: str, schema: str = "analytics"):
    if df is None or df.empty:
        logger.warning(f"Aucune donnée à charger dans {table_name}")
        return
        
    try:
        engine = get_engine()
        # Création du schéma si inexistant
        with engine.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.execute("COMMIT;")
            
        df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)
        logger.info(f"✅ Données chargées dans Postgres : {schema}.{table_name}")
    except Exception as e:
        logger.error(f"Erreur chargement Postgres ({schema}.{table_name}) : {e}")

def load_gold_data():
    base_dir = Path(__file__).resolve().parent.parent.parent
    gold_dir = base_dir / "data" / "gold"
    silver_dir = base_dir / "data" / "silver"

    # 1. Livres détaillés (silver enrichi avec devises)
    books_details_path = gold_dir / "books_details.parquet"
    if books_details_path.exists():
        df = pd.read_parquet(books_details_path)
        load_to_postgres(df, "gold_books_details")
    else:
        # Fallback : charger les livres silver directement
        silver_books = silver_dir / "books_clean.csv"
        if silver_books.exists():
            df = pd.read_csv(silver_books)
            load_to_postgres(df, "gold_books_details")

    # 2. Livres analytics (agrégations par catégorie)
    books_analytics_path = gold_dir / "books_analytics.parquet"
    if books_analytics_path.exists():
        df = pd.read_parquet(books_analytics_path)
        load_to_postgres(df, "gold_books_analytics")

    # 3. Pays enrichis (Countries + WorldBank + Rates)
    countries_path = gold_dir / "countries_enriched.parquet"
    if countries_path.exists():
        df = pd.read_parquet(countries_path)
        load_to_postgres(df, "gold_countries_enriched")

if __name__ == "__main__":
    load_gold_data()
