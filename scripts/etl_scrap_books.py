import requests
from bs4 import BeautifulSoup
import csv
import time
from utils.logging import get_logger, setup_logging
from typing import Optional, Dict, List
from pathlib import Path


# --------------------------------------------------
# CONFIGURATION LOGGING
# --------------------------------------------------
# def setup_logging(log_file: str = "app.log") -> None:
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         filename=log_file,
#         filemode="a"
#     )
#     logger = logging.getLogger("etl_scrap_books.py")

#     return logger


# logger = setup_logging()

setup_logging()

logger = get_logger(__name__)


file_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "book_links.txt" # noqa 501
file_path.parent.mkdir(parents=True, exist_ok=True)  # crée data/raw si besoin


# --------------------------------------------------
# UTILITAIRES
# --------------------------------------------------
def get_text_or_none(element) -> Optional[str]:
    return element.text.strip() if element else None


def read_links_from_file(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


# --------------------------------------------------
# SCRAPING
# --------------------------------------------------
def extract_book_info(url: str) -> Optional[Dict]:
    """Récupère les informations d'un livre à partir de son URL"""
    try:
        with requests.Session() as session:
            response = session.get(url, timeout=10)
            response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Erreur HTTP sur {url} : {e}")
        return None

    soup = BeautifulSoup(response.text, "html5lib")
    page = soup.find("div", class_="page")
    if not page:
        logger.warning(f"Structure HTML invalide pour {url}")
        return None

    def get_table_value(label: str) -> Optional[str]:
        th = page.find("th", string=label)
        return get_text_or_none(th.find_next_sibling("td")) if th else None

    description = None
    desc_block = page.find("div", id="product_description")
    if desc_block:
        description = get_text_or_none(desc_block.find_next_sibling("p"))

    return {
        "url": url,
        "titre": get_text_or_none(page.find("h1")),
        "description": description,
        "upc": get_table_value("UPC"),
        "type_produit": get_table_value("Product Type"),
        "prix_hors_taxe": get_table_value("Price (excl. tax)"),
        "prix_ttc": get_table_value("Price (incl. tax)"),
        "disponibilite": get_table_value("Availability"),
        "nombre_davis": get_table_value("Number of reviews"),
    }



file_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "book_links.txt" # noqa 501
output_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "books_raw_unique.csv" # noqa 501
file_path.parent.mkdir(parents=True, exist_ok=True)  # crée data/raw si besoin
output_path.parent.mkdir(parents=True, exist_ok=True)  # crée data/raw si besoin # noqa 501


# --------------------------------------------------
# SAUVEGARDE CSV
# --------------------------------------------------
# Extract to one csv file
def save_books_to_csv(
    links: List[str],
    output_path: str,
    delay: float = 0.2
) -> None:
    fieldnames = [
        "url", "titre", "description", "upc", "type_produit",
        "prix_hors_taxe", "prix_ttc", "disponibilite", "nombre_davis"
    ]

    with requests.Session() as session, \
            open(output_path, "w", newline="", encoding="utf-8") as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for i, link in enumerate(links, start=1):
            book = extract_book_info(session, link)

            if book:
                writer.writerow(book)
                logger.info(f"Livre {i} enregistré : {book['titre']}")
                print(f"📘 Livre {i} enregistré : {book['titre']}")
            else:
                logger.warning(f"Échec extraction livre {i} : {link}")
                print(f"❌ Échec livre {i}")

            time.sleep(delay)

    logger.info(f"Extraction terminée : {len(links)} livres")
    print(f"✅ Données sauvegardées dans {output_path}")


# Extract to many csv files
def save_books_info_to_csv(
    file_path,
    start_index=0,
    end_index=None,
    output_suffix=None
):
    # Lecture de tous les liens
    with open(file_path, "r", encoding="utf-8") as f:
        links = [line.strip() for line in f]

    # Découpage pour le parallélisme
    selected_links = links[start_index:end_index]

    # Nom de fichier spécifique par batch
    if output_suffix:
        file_path = f"data/raw/books_raw_{output_suffix}.csv"

    fieldnames = [
        "url", "titre", "description", "upc", "type_produit",
        "prix_hors_taxe", "prix_ttc", "disponibilite", "nombre_davis"
    ]

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, link in enumerate(selected_links, start=1):
            info = extract_book_info(link)

            if info:
                writer.writerow(info)
                logger.info(f"Livre batch enregistré : {info['titre']}")
            else:
                logger.warning(f"Échec extraction : {link}")

            time.sleep(0.2)

    logger.info(
        f"Batch terminé : {len(selected_links)} livres "
        f"({start_index} → {end_index})"
    )


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Début de l'extractions des informations ou details")
    logger.info("Démarrage du scraping")

    links = read_links_from_file(file_path)
    save_books_to_csv(links)

    # Batchs de 100
    save_books_info_to_csv(start_index=0, end_index=100, output_suffix="batch1") # noqa 501
    save_books_info_to_csv(start_index=100, end_index=200, output_suffix="batch2") # noqa 501
    save_books_info_to_csv(start_index=200, end_index=300, output_suffix="batch3") # noqa 501
    save_books_info_to_csv(start_index=300, end_index=400, output_suffix="batch4") # noqa 501
    save_books_info_to_csv(start_index=400, end_index=500, output_suffix="batch5") # noqa 501
    save_books_info_to_csv(start_index=500, end_index=600, output_suffix="batch6") # noqa 501
    save_books_info_to_csv(start_index=600, end_index=700, output_suffix="batch7") # noqa 501
    save_books_info_to_csv(start_index=700, end_index=800, output_suffix="batch8") # noqa 501
    save_books_info_to_csv(start_index=800, end_index=900, output_suffix="batch9")# noqa 501
    save_books_info_to_csv(start_index=900, end_index=1000, output_suffix="batch10")# noqa 501

    logger.info("Scraping terminé avec succès")
    print("Finde l'extractionde informations")


if __name__ == "__main__":
    main()
