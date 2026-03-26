import pandas as pd
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
COUNTRIES_SILVER = BASE_DIR / "data" / "silver" / "countries" / "countries_silver.parquet"
POPULATION_RAW = BASE_DIR / "data" / "raw" / "worldbank" / "population_all.parquet"
RATES_SILVER = BASE_DIR / "data" / "silver" / "exchange_rates" / "rates_silver.parquet"
GOLD_DIR = BASE_DIR / "data" / "gold"

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)


# -------------------------
# GOLD : COUNTRIES ENRICHED
# -------------------------
def create_gold_countries_enriched() -> pd.DataFrame:
    """
    Jointure naturelle :
      - Countries API (iso2, region, population, area, currency_codes)
      - WorldBank Population (dernière année disponible, via iso2 <-> country_iso2)
      - Exchange Rates (taux de change de la devise principale du pays)
    """
    try:
        logger.info("Début création Gold Countries Enriched")

        # --------------------------------------------------
        # 1. Chargement des pays
        # --------------------------------------------------
        if not COUNTRIES_SILVER.exists():
            logger.error(f"Fichier introuvable : {COUNTRIES_SILVER}")
            return None

        df_countries = pd.read_parquet(COUNTRIES_SILVER)
        logger.info(f"Countries chargés : {len(df_countries)} lignes")

        # --------------------------------------------------
        # 2. Jointure avec WorldBank Population (via iso2)
        # --------------------------------------------------
        if POPULATION_RAW.exists():
            df_pop = pd.read_parquet(POPULATION_RAW)

            # Prendre la population la plus récente par pays
            latest_year = df_pop.groupby("country_iso2")["year"].max().reset_index()
            latest_year.columns = ["country_iso2", "latest_year"]

            df_pop_latest = pd.merge(df_pop, latest_year,
                                     left_on=["country_iso2", "year"],
                                     right_on=["country_iso2", "latest_year"])

            df_pop_latest = df_pop_latest[["country_iso2", "population", "latest_year"]].rename(columns={
                "population": "population_banque_mondiale",
                "latest_year": "annee_population",
            })

            # Jointure Countries <-> WorldBank via iso2
            df_countries = pd.merge(
                df_countries,
                df_pop_latest,
                left_on="iso2",
                right_on="country_iso2",
                how="left"
            )

            # Supprimer la colonne doublon
            if "country_iso2" in df_countries.columns:
                df_countries.drop(columns=["country_iso2"], inplace=True)

            logger.info("Jointure WorldBank Population effectuée")
        else:
            df_countries["population_banque_mondiale"] = None
            df_countries["annee_population"] = None
            logger.warning("Données WorldBank non disponibles")

        # --------------------------------------------------
        # 3. Jointure avec Exchange Rates (via currency_codes)
        # --------------------------------------------------
        if RATES_SILVER.exists():
            df_rates = pd.read_parquet(RATES_SILVER)

            # Extraire la devise principale de chaque pays
            df_countries["devise_principale"] = (
                df_countries["currency_codes"]
                .str.split(",")
                .str[0]
                .str.strip()
            )

            # Construire un mapping devise -> taux
            rate_columns = [c for c in df_rates.columns if c not in ["processed_at"]]
            currency_rates = {}
            for col in rate_columns:
                try:
                    currency_rates[col] = float(df_rates[col].iloc[0])
                except (ValueError, IndexError):
                    pass

            df_countries["taux_change_vs_gbp"] = (
                df_countries["devise_principale"].map(currency_rates)
            )

            logger.info("Jointure Exchange Rates effectuée")
        else:
            df_countries["devise_principale"] = None
            df_countries["taux_change_vs_gbp"] = None
            logger.warning("Données Exchange Rates non disponibles")

        # --------------------------------------------------
        # 4. Calculs dérivés
        # --------------------------------------------------
        # Renommer pour clarté
        if "population" in df_countries.columns:
            df_countries = df_countries.rename(columns={"population": "population_api_countries"})

        if "name" in df_countries.columns:
            df_countries = df_countries.rename(columns={"name": "pays"})

        # Densité de population (à partir des données REST Countries)
        if "population_api_countries" in df_countries.columns and "area" in df_countries.columns:
            df_countries["densite_population"] = (
                df_countries["population_api_countries"] / df_countries["area"]
            ).round(2)

        # Renommer les autres colonnes pour clarté
        rename_map = {
            "official_name": "nom_officiel",
            "region": "region",
            "subregion": "sous_region",
            "capital": "capitale",
            "area": "superficie_km2",
            "languages": "langues",
            "currencies": "devises",
            "currency_codes": "codes_devises",
        }
        df_countries = df_countries.rename(columns={k: v for k, v in rename_map.items() if k in df_countries.columns})

        df_countries["processed_at"] = datetime.utcnow()

        # --------------------------------------------------
        # 5. Sélection et ordre des colonnes
        # --------------------------------------------------
        desired_cols = [
            "pays", "nom_officiel", "iso2", "iso3",
            "region", "sous_region", "capitale",
            "superficie_km2", "population_api_countries",
            "population_banque_mondiale", "annee_population",
            "densite_population",
            "langues", "devises", "codes_devises",
            "devise_principale", "taux_change_vs_gbp",
            "processed_at",
        ]
        # Garder uniquement les colonnes qui existent
        final_cols = [c for c in desired_cols if c in df_countries.columns]
        df_countries = df_countries[final_cols]

        # --------------------------------------------------
        # 6. Sauvegarde
        # --------------------------------------------------
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = GOLD_DIR / "countries_enriched.parquet"
        df_countries.to_parquet(file_path, index=False)

        logger.info(f"✅ Gold Countries Enriched créé : {len(df_countries)} pays")
        return df_countries

    except Exception as e:
        logger.error(f"Erreur Gold Countries Enriched : {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    create_gold_countries_enriched()
