import requests
from bs4 import BeautifulSoup
import time
from typing import List, Optional
from pathlib import Path
from include.utils.custom_logging import setup_logging, get_logger

BASE_URL = "https://books.toscrape.com/"


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


# --------------------------------------------------
# HTTP / SESSION
# --------------------------------------------------
def create_session() -> requests.Session:
    """
    Crée et configure une session HTTP.
    Séparée pour être testable et compatible Airflow.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": "BooksETL/1.0"
    })
    return session


# --------------------------------------------------
# HTTP / PARSING
# --------------------------------------------------
def get_soup(session: requests.Session, url: str) -> Optional[BeautifulSoup]:
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return BeautifulSoup(response.text, "html5lib")
    except requests.RequestException as e:
        logger.error(f"Erreur HTTP sur {url} : {e}")
        return None


# --------------------------------------------------
# EXTRACTION
# --------------------------------------------------
def get_all_book_links(session: requests.Session) -> List[str]:
    # Skip scraping si le fichier de liens existe déjà
    if LINKS_FILE.exists() and LINKS_FILE.stat().st_size > 5000:
        logger.info("✅ Liens déjà extraits. Saut du scraping réseau.")
        return []
        
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
        time.sleep(0.2)

    return book_links


# --------------------------------------------------
# SAUVEGARDE
# --------------------------------------------------
def save_book_links(file_path: Path, links: List[str]) -> None:
    # Skip si le fichier existe et est consistant (1000 liens environ)
    if file_path.exists() and file_path.stat().st_size > 5000:
        logger.info(f"✅ Le fichier {file_path} existe déjà. Saut de l'écriture.")
        return

    with open(file_path, "w", encoding="utf-8") as f:
        for link in links:
            f.write(link + "\n")
    if links:
        logger.info(f"{len(links)} liens sauvegardés dans {file_path}")

    else:
        logger.warning("Aucun lien trouvé")


# --------------------------------------------------
# EXECUTION LOCALE (HORS AIRFLOW)
# --------------------------------------------------
def main():
    if LINKS_FILE.exists() and LINKS_FILE.stat().st_size > 5000:
        logger.info("✅ Le fichier de liens existe déjà et semble complet. Saut du scraping.")
        return

    session = create_session()
    save_book_links(LINKS_FILE, links=get_all_book_links(session))
    print("Extraction des liens terminés")
    logger.info("Extraction des liens terminés")


if __name__ == "__main__":
    main()
