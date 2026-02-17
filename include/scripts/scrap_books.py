import csv
import time
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from typing import Optional, Dict, List
import json

from include.scripts.scrap_links import create_session
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# PATHS
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LINKS_FILE = DATA_DIR / "book_links.txt"
OUTPUT_CSV = DATA_DIR / "books_raw_unique.csv"
OUTPUT_JSON = DATA_DIR / "books_raw_unique.json"


# -------------------------
# UTILITAIRES
# -------------------------
def get_text_or_none(element) -> Optional[str]:
    return element.text.strip() if element else None


def read_links_from_file(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")

    with open(path, "r", encoding="utf-8") as f:
        links = [line.strip() for line in f if line.strip()]
    return links


# -------------------------
# SESSION
# -------------------------
session = create_session()


# -------------------------
# SCRAPING
# -------------------------
def extract_book_infos(
    url: str,
    session: requests.Session
) -> Dict:

    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Erreur HTTP sur {url} : {e}")
        raise

    soup = BeautifulSoup(response.text, "html.parser")

    product_main = soup.find("div", class_="product_main")
    if not product_main:
        raise ValueError(f"Structure HTML invalide pour {url}")

    def get_table_value(label: str) -> Optional[str]:
        th = soup.find("th", string=label)
        return get_text_or_none(th.find_next_sibling("td")) if th else None

    description = None
    desc_block = soup.find("div", id="product_description")
    if desc_block:
        description = get_text_or_none(desc_block.find_next_sibling("p"))

    return {
        "url": url,
        "titre": get_text_or_none(product_main.find("h1")),
        "description": description,
        "upc": get_table_value("UPC"),
        "type_produit": get_table_value("Product Type"),
        "prix_hors_taxe": get_table_value("Price (excl. tax)"),
        "prix_ttc": get_table_value("Price (incl. tax)"),
        "disponibilite": get_table_value("Availability"),
        "nombre_davis": get_table_value("Number of reviews"),
    }


# -------------------------
# SAUVEGARDE JSON
# -------------------------
def save_books_info_to_json(
    output_path: Path,
    delay: float = 0.2
) -> None:

    links = read_links_from_file(LINKS_FILE)
    logger.info(f"{len(links)} liens chargés depuis {LINKS_FILE}")

    books = []

    with requests.Session() as session:
        for i, link in enumerate(links, start=1):
            try:
                book = extract_book_infos(link, session)
                books.append(book)

                logger.info(f"Livre {i} extrait : {book['titre']}")
                print(f"📘 Livre {i} extrait : {book['titre']}")

            except Exception as e:
                logger.warning(f"Échec extraction livre {i} ({link}) : {e}")

            time.sleep(delay)

    with open(output_path, "w", encoding="utf-8") as jsonfile:
        json.dump(books, jsonfile, ensure_ascii=False, indent=4)

    logger.info(f"Extraction terminée : {len(books)} livres")
    print(f"✅ Données sauvegardées dans {output_path}")


# -------------------------
# SAUVEGARDE CSV
# -------------------------
def save_books_info_to_csv(
    output_path: Path,
    delay: float = 0.2
) -> None:

    fieldnames = [
        "url", "titre", "description", "upc", "type_produit",
        "prix_hors_taxe", "prix_ttc", "disponibilite", "nombre_davis"
    ]

    links = read_links_from_file(LINKS_FILE)
    logger.info(f"{len(links)} liens chargés depuis {LINKS_FILE}")

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        with requests.Session() as session:
            for i, link in enumerate(links, start=1):
                try:
                    book = extract_book_infos(link, session)
                    writer.writerow(book)

                    logger.info(f"Livre {i} enregistré : {book['titre']}")
                    print(f"📘 Livre {i} enregistré : {book['titre']}")

                except Exception as e:
                    logger.warning(f"Échec extraction livre {i} ({link}) : {e}") # noqa 501

                time.sleep(delay)

    logger.info(f"Extraction terminée : {len(links)} livres")
    print(f"✅ Données sauvegardées dans {output_path}")


# -------------------------
# MAIN
# -------------------------
def main():
    logger.info("Démarrage du scraping")

    logger.info("Extraction des informations des livres")
    session = create_session()
    extract_book_infos(session)
    session.close()

    logger.info("Sauvegarde des données au format JSON")
    save_books_info_to_json(
        output_path=OUTPUT_JSON
    )

    logger.info("Sauvegarde des données au format CSV")
    save_books_info_to_csv(
        output_path=OUTPUT_CSV
    )

    logger.info("✅ Scraping terminé avec succès")


if __name__ == "__main__":
    main()
