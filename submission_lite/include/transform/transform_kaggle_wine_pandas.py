import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger


# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_PATH = BASE_DIR / "data" / "raw" / "wine" / "wine_reviews.csv"
SILVER_PATH = BASE_DIR / "data" / "silver" / "wine"
AUDIT_DIR = BASE_DIR / "data" / "audit"


# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)


def transform_wine_pandas(input_path: Path,
                          output_path: Path):

    df = pd.read_parquet(input_path)

    # -------------------------
    # Nettoyage
    # -------------------------
    df = df.drop_duplicates(subset=["title"])

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["points"] = pd.to_numeric(df["points"], errors="coerce")

    # Supprimer lignes sans prix ou pays
    df = df[df["price"].notna()]
    df = df[df["country"].notna()]

    # Normalisation texte
    df["country"] = df["country"].str.strip()
    df["variety"] = df["variety"].str.strip()

    # Ajout colonne business
    df["price_per_point"] = df["price"] / df["points"]

    df["processed_at"] = datetime.utcnow()

    # -------------------------
    # Sauvegarde Silver
    # -------------------------
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        output_path,
        index=False,
        compression="snappy"
    )

    print(f"✅ Silver Wine sauvegardé : {output_path}")
    
def main():
    df = transform_wine_pandas(RAW_PATH, SILVER_PATH)
    logger.info("✅ Transformation Wine terminée")

if __name__ == "__main__":
    main()