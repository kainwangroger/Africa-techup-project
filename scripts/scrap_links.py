import requests
from bs4 import BeautifulSoup
import time
from typing import List, Optional
from utils.logging import get_logger, setup_logging
from pathlib import Path


BASE_URL = "https://books.toscrape.com/"


# --------------------------------------------------
# LOGGING
# --------------------------------------------------
# def setup_logging(log_file: str = "app.log") -> None:
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         filename=log_file,
#         filemode="a",
#     )

#     logger = logging.getLogger("scrap_all_links.py")

#     return logger
# logger = setup_logging()


setup_logging()

logger = get_logger(__name__)


# --------------------------------------------------
# HTTP / PARSING
# --------------------------------------------------
def get_soup(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    """Retourne BeautifulSoup si l'URL est valide, sinon None"""
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html5lib")
    except requests.RequestException as e:
        logger.error(f"Erreur HTTP sur {url} : {e}")
        return None


# --------------------------------------------------
# EXTRACTION LIENS
# --------------------------------------------------
def get_all_book_links(session: requests.Session) -> List[str]:
    """
    Récupère les liens des 1000 livres (50 pages)
    """
    logger.info("Début extraction des liens")
    book_links = []

    for page in range(1, 51):
        url = f"{BASE_URL}catalogue/page-{page}.html"

        soup = get_soup(session, url)
        if not soup:
            continue

        books = soup.select("article.product_pod h3 a")

        for book in books:
            relative_link = book["href"].replace("../../", "")
            full_link = f"{BASE_URL}catalogue/{relative_link}"
            book_links.append(full_link)

        logger.info(f"Page {page} récupérée ({len(books)} livres)")
        print(f"📄 Page {page} récupérée ({len(books)} livres)")

        time.sleep(0.2)

    return book_links


file_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "book_links.txt" # noqa 501
file_path.parent.mkdir(parents=True, exist_ok=True)  # crée data/raw si besoin


# --------------------------------------------------
# SAUVEGARDE
# --------------------------------------------------
def save_book_links(
    file_path: str
) -> None:
    """Sauvegarde tous les liens de livres dans un fichier texte"""

    with requests.Session() as session:
        links = get_all_book_links(session)

    with open(file_path, "w", encoding="utf-8") as f:
        for link in links:
            f.write(link + "\n")

    logger.info(f"{len(links)} liens sauvegardés dans {file_path}")
    print(f"✅ {len(links)} liens sauvegardés dans {file_path}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main():
    print("Début du scrapping des liens")
    logger.info("Début du scraping des liens")

    save_book_links()

    logger.info("Fin du scraping des liens")
    print("🏁 Fin du scraping")


if __name__ == "__main__":
    main()
