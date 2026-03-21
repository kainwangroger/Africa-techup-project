import zipfile
import pandas as pd
from pathlib import Path
from typing import Optional
from kaggle.api.kaggle_api_extended import KaggleApi
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
DATASET = "zynicide/wine-reviews"
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "kaggle"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# EXTRACTION
# -------------------------
def extract_wine_reviews() -> Optional[pd.DataFrame]:

    logger.info("Début extraction Kaggle Wine Reviews")

    api = KaggleApi()
    api.authenticate()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Télécharger dataset Kaggle
    api.dataset_download_files(DATASET, path=RAW_DIR, unzip=False)

    # Trouver le zip et l'extraire
    zip_file = list(RAW_DIR.glob("*.zip"))[0]
    with zipfile.ZipFile(zip_file, "r") as z:
        z.extractall(RAW_DIR)

    # Trouver le CSV
    csv_file = list(RAW_DIR.glob("*.csv"))[0]

    df = pd.read_csv(csv_file)

    logger.info(f"{len(df)} lignes extraites depuis Kaggle")

    # Supprimer le zip après extraction pour éviter le tmp
    zip_file.unlink()

    return df

# -------------------------
# SAUVEGARDE RAW CSV
# -------------------------
def save_wine_reviews_csv(df: pd.DataFrame) -> None:

    if df is None or df.empty:
        logger.warning("Aucune donnée à sauvegarder")
        return

    file_path = RAW_DIR / "wine_reviews.csv"
    df.to_csv(file_path, index=False)
    logger.info(f"Données RAW sauvegardées dans {file_path}")

# -------------------------
# MAIN
# -------------------------
def main():
    df = extract_wine_reviews()
    save_wine_reviews_csv(df)
    logger.info("Extraction Kaggle terminée")

if __name__ == "__main__":
    main()