import requests
import pandas as pd
from typing import Optional
from datetime import datetime
from pathlib import Path
from include.utils.custom_logging import setup_logging, get_logger


# -------------------------
# CONFIG
# -------------------------
BASE_URL = "https://restcountries.com/v3.1/all"
BASE_DIR = Path(__file__).resolve().parent.parent.parent

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
    session.headers.update({
        "User-Agent": "CountriesETL/1.0"
    })
    return session


# -------------------------
# EXTRACTION
# -------------------------
def extract_countries(session: requests.Session) -> Optional[pd.DataFrame]:

    logger.info("Début extraction Countries API")

    try:
        # Champs max 10
        FIELDS = [
            "name",
            "capital",
            "region",
            "subregion",
            "population",
            "area",
            "languages",
            "currencies",
            "cca2",
            "cca3",
        ]
        params = {"fields": ",".join(FIELDS)}

        response = session.get(BASE_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        records = []

        for country in data:
            languages = (
                ", ".join(country.get("languages", {}).values())
                if country.get("languages") else None
            )

            currency_codes = (
                ", ".join(country.get("currencies", {}).keys())
                if country.get("currencies") else None
            )

            currencies = (
                ", ".join(
                    [v.get("name") for v in country.get("currencies", {}).values()]
                )
                if country.get("currencies") else None
            )

            records.append({
                "name": country.get("name", {}).get("common"),
                "official_name": country.get("name", {}).get("official"),
                "region": country.get("region"),
                "subregion": country.get("subregion"),
                "population": country.get("population"),
                "area": country.get("area"),
                "capital": country.get("capital", [None])[0] if country.get("capital") else None,
                "languages": languages,
                "currencies": currencies,
                "currency_codes": currency_codes,
                "iso2": country.get("cca2"),
                "iso3": country.get("cca3"),
            })

        df = pd.DataFrame(records)
        logger.info(f"{len(df)} pays extraits avec enrichissement complet")
        return df

    except requests.RequestException as e:
        logger.error(f"Erreur HTTP : {e}")
        return None


# -------------------------
# SAUVEGARDE SIMPLE
# -------------------------
def save_countries_data(base_dir: Path, df: pd.DataFrame) -> None:

    if df is None or df.empty:
        logger.warning("Aucune donnée à sauvegarder")
        return

    output_dir = base_dir / "data" / "raw" / "countries"
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / "data_countries.parquet"

    df.to_parquet(file_path, index=False, compression="snappy")

    logger.info(f"{len(df)} pays sauvegardés dans {file_path}")


# -------------------------
# MAIN
# -------------------------
def main():
    output_file = BASE_DIR / "data" / "raw" / "countries" / "data_countries.parquet"
    if output_file.exists():
        # Skip si le fichier a été modifié aujourd'hui
        mtime = datetime.fromtimestamp(output_file.stat().st_mtime).date()
        if mtime == datetime.utcnow().date():
            logger.info("✅ Données Countries déjà extraites aujourd'hui. Saut de l'API.")
            return

    session = create_session()
    df = extract_countries(session)
    save_countries_data(BASE_DIR, df)
    logger.info("Pipeline Countries terminé")


if __name__ == "__main__":
    main()