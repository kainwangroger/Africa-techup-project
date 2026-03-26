import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
BOOKS_SILVER = BASE_DIR / "data" / "silver" / "books_clean.csv"
RATES_RAW = BASE_DIR / "data" / "raw" / "exchange_rates" / "rates_gbp.parquet"
GOLD_DIR = BASE_DIR / "data" / "gold"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)


# -------------------------
# GOLD : BOOKS DETAILS
# -------------------------
def create_gold_books_details() -> pd.DataFrame:
    """
    Enrichit les livres silver avec les prix en EUR et USD.
    Table détaillée : 1 ligne = 1 livre.
    """
    try:
        logger.info("Début création Gold Books Details")

        if not BOOKS_SILVER.exists():
            logger.error(f"Fichier introuvable : {BOOKS_SILVER}")
            return None

        df = pd.read_csv(BOOKS_SILVER)

        # Conversion de devises si les taux sont disponibles
        if RATES_RAW.exists():
            try:
                rates_df = pd.read_parquet(RATES_RAW)
                if "EUR" in rates_df.columns:
                    df["price_eur"] = (df["price_gbp"] * rates_df["EUR"].iloc[0]).round(2)
                if "USD" in rates_df.columns:
                    df["price_usd"] = (df["price_gbp"] * rates_df["USD"].iloc[0]).round(2)
                logger.info("Conversion EUR/USD effectuée")
            except Exception as e:
                logger.warning(f"Conversion devises ignorée : {e}")
        else:
            logger.warning("Taux de change non disponibles, pas de conversion")

        df["processed_at"] = datetime.utcnow()

        # Sauvegarde
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = GOLD_DIR / "books_details.parquet"
        df.to_parquet(file_path, index=False)

        logger.info(f"✅ Gold Books Details créé : {len(df)} livres")
        return df

    except Exception as e:
        logger.error(f"Erreur Gold Books Details : {e}")
        return None


# -------------------------
# GOLD : BOOKS ANALYTICS
# -------------------------
def create_gold_books_analytics() -> pd.DataFrame:
    """
    Agrégations par catégorie de livres.
    Table analytique : 1 ligne = 1 catégorie.
    """
    try:
        logger.info("Début création Gold Books Analytics")

        if not BOOKS_SILVER.exists():
            logger.error(f"Fichier introuvable : {BOOKS_SILVER}")
            return None

        df = pd.read_csv(BOOKS_SILVER)

        # Agrégation par catégorie
        agg = df.groupby("category").agg(
            nb_books=("category", "count"),
            avg_price_gbp=("price_gbp", "mean"),
            min_price_gbp=("price_gbp", "min"),
            max_price_gbp=("price_gbp", "max"),
            total_stock=("stock", "sum"),
            avg_reviews=("reviews", "mean"),
        ).reset_index()

        # Arrondir les colonnes numériques
        for col in ["avg_price_gbp", "min_price_gbp", "max_price_gbp", "avg_reviews"]:
            agg[col] = agg[col].round(2)

        # Enrichir avec les taux de change
        if RATES_RAW.exists():
            try:
                rates_df = pd.read_parquet(RATES_RAW)
                if "EUR" in rates_df.columns:
                    eur_rate = rates_df["EUR"].iloc[0]
                    agg["avg_price_eur"] = (agg["avg_price_gbp"] * eur_rate).round(2)
                if "USD" in rates_df.columns:
                    usd_rate = rates_df["USD"].iloc[0]
                    agg["avg_price_usd"] = (agg["avg_price_gbp"] * usd_rate).round(2)
            except Exception as e:
                logger.warning(f"Conversion devises ignorée : {e}")

        agg["processed_at"] = datetime.utcnow()

        # Sauvegarde
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = GOLD_DIR / "books_analytics.parquet"
        agg.to_parquet(file_path, index=False)

        logger.info(f"✅ Gold Books Analytics créé : {len(agg)} catégories")
        return agg

    except Exception as e:
        logger.error(f"Erreur Gold Books Analytics : {e}")
        return None


if __name__ == "__main__":
    create_gold_books_details()
    create_gold_books_analytics()
