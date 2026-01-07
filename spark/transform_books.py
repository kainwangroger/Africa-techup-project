import pandas as pd
import os
import re
from utils.logging import setup_logging, get_logger
from pathlib import Path


# --------------------------------------------------
# LOGGING
# --------------------------------------------------
# def setup_logging(log_file: str = "app.log") -> None:
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         filename=log_file,
#         filemode="a"
#     )
#     logger = logging.getLogger("transform_books.py")

#     return logger


# logger = setup_logging()

setup_logging()
logger = get_logger(__name__)

file_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "books_raw.csv" # noqa 501
file_path.parent.mkdir(parents=True, exist_ok=True)  # crée data/raw si besoin
output_path = Path(__file__).resolve().parent.parent / "data" / "processed" / "books_clean.csv" # noqa 501
output_path.parent.mkdir(parents=True, exist_ok=True)  # crée data/processed si besoin # noqa 501


# --------------------------------------------------
# NETTOYAGE
# --------------------------------------------------
def clean_price(value):
    """£51.77 -> 51.77"""
    if pd.isna(value):
        return None
    return float(value.replace("£", "").strip())


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
    input_path: str = file_path,
    output_path: str = "data/processed/books_clean.csv"
) -> None:

    logger.info("Transformation PySpark démarrée")

    if not os.path.exists(input_path):
        logger.error(f"Fichier introuvable : {input_path}")
        print("❌ Fichier RAW introuvable")
        return

    logger.info("Début transformation des données")

    df = pd.read_csv(input_path)

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
    logger.info("Pipeline démarré")
    transform_books()

    logger.info("Pipeline terminés")
    print("Fin du pipline")


if __name__ == "__main__":
    main()
