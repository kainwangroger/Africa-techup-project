import pandas as pd
import os
import re
from include.utils.custom_logging import setup_logging, get_logger
from pathlib import Path


# --------------------------------------------------
# LOGGING
# --------------------------------------------------
setup_logging()
logger = get_logger(__name__)

# --------------------------------------------------
# PATHS
# --------------------------------------------------

BATCH_SIZE = 100
TOTAL_BOOKS = 1000

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LINKS_FILE = DATA_DIR / "raw" / "book_links.txt"
CSV_FILES = DATA_DIR / "raw" / "books_raw_unique.csv"
JSON_FILES = DATA_DIR / "raw" / "books_raw_unique.json"

OUTPUT_FILES = DATA_DIR / "processed" / "books_clean.csv"


# --------------------------------------------------
# NETTOYAGE
# --------------------------------------------------
# def clean_price(value):
#     """£51.77 -> 51.77"""
#     if pd.isna(value):
#         return None
#     return float(value.replace("£", "").strip())

def clean_price(value: str) -> float:
    if not isinstance(value, str):
        return float(value)
    # Supprimer les caractères indésirables
    cleaned = (
        value.replace("£", "")
             .replace("Â", "")
             .strip()
    )
    try:
        return float(cleaned)
    except ValueError:
        # Retourner NaN si la conversion échoue
        return float("nan")


def clean_availability(value):
    """In stock (22 available) -> 22"""
    if pd.isna(value):
        return 0

    match = re.search(r"\((\d+)", value)
    return int(match.group(1)) if match else 0


def clean_reviews(value):
    """Convert number of reviews to int"""
    if pd.isna(value):
        return 0
    return int(value)


# --------------------------------------------------
# PIPELINE TRANSFORM
# --------------------------------------------------
def transform_books(
    input_path: str = CSV_FILES,
    output_path: str = OUTPUT_FILES
) -> None:

    logger.info("Transformation PySpark démarrée")

    if not os.path.exists(input_path):
        logger.error(f"Fichier introuvable : {input_path}")
        print("Fichier RAW introuvable")
        return

    logger.info("Début transformation des données")

    df = pd.read_csv(input_path, encoding="utf-8")

    # Nettoyage colonnes
    df["prix_hors_taxe"] = df["prix_hors_taxe"].apply(clean_price)
    df["prix_ttc"] = df["prix_ttc"].apply(clean_price)
    df["disponibilite"] = df["disponibilite"].apply(clean_availability)
    df["nombre_davis"] = df["nombre_davis"].apply(clean_reviews)

    # Renommage clair
    df = df.rename(columns={
        "prix_hors_taxe": "price_excl_tax",
        "prix_ttc": "price_incl_tax",
        "disponibilite": "stock",
        "nombre_davis": "reviews"
    })

    # Création dossier si nécessaire
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)

    logger.info(f"Transformation terminée : {len(df)} lignes")
    print(f"✅ Données nettoyées sauvegardées dans {output_path}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Début de transformation avec pyspark")
    logger.info("Pipeline  traitements des données démarré")
    transform_books(CSV_FILES, OUTPUT_FILES)

    logger.info("Pipeline terminés")
    print("Fin du pipeline")


# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    main()
