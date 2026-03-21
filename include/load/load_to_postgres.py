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
    gold_path = base_dir / "data" / "gold" / "project_summary.parquet"
    
    if gold_path.exists():
        df = pd.read_parquet(gold_path)
        load_to_postgres(df, "gold_project_summary")
        
    # On peut aussi charger les livres silver pour Grafana
    silver_books = base_dir / "data" / "silver" / "books_clean.csv"
    if silver_books.exists():
        df_books = pd.read_csv(silver_books)
        load_to_postgres(df_books, "gold_books_details")

if __name__ == "__main__":
    load_gold_data()
