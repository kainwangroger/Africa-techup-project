import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger
from typing import Optional


# -------------------------
# CONFIG
# -------------------------
BASE_URL = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
PARAMS = {"format": "json", "per_page": 1000}
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "worldbank"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)


# -------------------------
# SESSION
# -------------------------
def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "WorldBankETL/1.0"})
    return session


# -------------------------
# EXTRACTION RAW CSV
# -------------------------
def extract_population_all(session: requests.Session) -> Optional[pd.DataFrame]:
    logger.info("Début extraction population mondiale (RAW CSV)")
    page = 1
    all_records = []

    while True:
        PARAMS["page"] = page
        try:
            response = session.get(BASE_URL, params=PARAMS, timeout=20)
            response.raise_for_status()
            json_data = response.json()
        except requests.RequestException as e:
            logger.error(f"Erreur HTTP : {e}")
            return None

        metadata, data = json_data[0], json_data[1]

        if not data:
            break

        for item in data:
            if item["value"] is not None:
                all_records.append({
                    "indicator_id": item["indicator"]["id"],
                    "country_iso2": item["country"]["id"],
                    "country_name": item["country"]["value"],
                    "year": int(item["date"]),
                    "population": item["value"]
                })

        logger.info(f"Page {page} récupérée")
        if page >= metadata["pages"]:
            break
        page += 1

    df = pd.DataFrame(all_records)
    logger.info(f"{len(df)} lignes extraites au total")
    return df

# -------------------------
# SAUVEGARDE RAW CSV
# -------------------------
def save_population_raw(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        logger.warning("Aucune donnée RAW à sauvegarder")
        return

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    file_path = RAW_DIR / "population_all.parquet"
    df.to_parquet(file_path, index=False, compression="snappy")
    logger.info(f"Données RAW sauvegardées : {file_path}")


# -------------------------
# MAIN
# -------------------------
def main():
    output_file = RAW_DIR / "population_all.parquet"
    if output_file.exists():
        mtime = datetime.fromtimestamp(output_file.stat().st_mtime).date()
        if mtime == datetime.utcnow().date():
            logger.info("✅ Données WorldBank déjà extraites aujourd'hui. Saut de l'API.")
            return

    session = create_session()
    df_raw = extract_population_all(session)
    save_population_raw(df_raw)
    logger.info("Extraction population RAW terminée")

if __name__ == "__main__":
    main()