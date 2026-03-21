import requests
from bs4 import BeautifulSoup
import csv
import time
import logging
from minio import Minio
import os
from dotenv import load_dotenv
# from minio.error import S3Error


logging.basicConfig(
    level=logging.DEBUG,                      # Niveau minimal de log
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format du message
    filename="app.log",                       # Fichier de sortie (optionnel)
    filemode="a"                              # 'w' = écraser, 'a' = ajouter
)

print("Début du scraping.")
logging.info("Début du scraping…")


def get_text_or_none(soup_element):
    if soup_element:
        return soup_element.text.strip()
    return None


logging.info("Extraction des informations de chaque url...")


def extract_book_info(url):
    """Récupère toutes les infos d'un livre depuis son URL."""
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erreur {response.status_code} sur {url}")
        logging.error(f"Erreur {response.status_code} sur {url}")
        return None

    soup = BeautifulSoup(response.text, "html5lib")
    div_bloc = soup.find('div', class_='page')
    if div_bloc is None:
        return None

    # Récupération des données
    titre_article = get_text_or_none(div_bloc.find('h1'))

    description_article = None
    description_bloc = div_bloc.find('div', id='product_description')
    if description_bloc:
        description_article = get_text_or_none(
            description_bloc.find_next_sibling('p'))

    def get_table_value(label):
        bloc = div_bloc.find('th', string=label)
        if bloc:
            valeur = bloc.find_next_sibling('td')
            return get_text_or_none(valeur)
        return None

    upc = get_table_value("UPC")
    product_type = get_table_value("Product Type")
    price_excl_tax = get_table_value("Price (excl. tax)")
    price_incl_tax = get_table_value("Price (incl. tax)")
    availability = get_table_value("Availability")
    num_reviews = get_table_value("Number of reviews")

    return {
        "url": url,
        "titre": titre_article,
        "description": description_article,
        "upc": upc,
        "type_produit": product_type,
        "prix_hors_taxe": price_excl_tax,
        "prix_ttc": price_incl_tax,
        "disponibilite": availability,
        "nombre_davis": num_reviews
    }


logging.info("Extraction des informations des url terminés avec succès...")

# --- Lire les liens depuis le fichier txt ---
with open("../data/raw/book_links.txt", "r", encoding="utf-8") as f:
    links = [line.strip() for line in f]

logging.info("Sauvagarde des informations rextraits dans un fichier csv...")


def save_book_info_to_csv(file_path="data/raw/books_raw.csv"):
    """
    Extrait les infos de chaque livre et \
    les sauvegarde dans un CSV.
    """
    with open(file_path, "w", newline='', encoding='utf-8') as f:
        fieldnames = ["url", "titre", "description", "upc", "type_produit",
                      "prix_hors_taxe", "prix_ttc",
                      "disponibilite", "nombre_davis"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, link in enumerate(links, start=1):
            info = extract_book_info(link)
            if info:
                writer.writerow(info)
                print(f"Livre {i} enregistré : {info['titre']}")
                logging.info(f"Livre {i} enregistré : {info['titre']}")
            else:
                print(f"Échec pour le livre {i} : {link}")
                logging.warning(f"Livre {i} enregistré : {info['titre']}")
            time.sleep(0.2)  # pause pour ne pas spammer le site

    print(f"✅ Tous les livres ont été sauvegardés dans {file_path}")


logging.info("Sauvagarde des informations terminés avec succès...")

# Exemple d'utilisation
if __name__ == "__main__":
    save_book_info_to_csv()

###########################################################################

logging.basicConfig(
    level=logging.DEBUG,                      # Niveau minimal de log
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format du message
    filename="app.log",                       # Fichier de sortie (optionnel)
    filemode="a"                              # 'w' = écraser, 'a' = ajouter
)

print("Début du scraping.")
logging.info("Début du scraping...")

BASE_URL = "https://books.toscrape.com/"

logging.info("Checking de l'url du site...")


def get_soup(url):
    """
        Verifie si l'url est valide sinon retourne None
    Args:
        url (_type_): _description_

    Returns:
        _type_: _description_
    """
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erreur {response.status_code} sur {url}")
        logging.warning(f"Erreur {response.status_code} sur {url}")

        return None
    return BeautifulSoup(response.text, "html.parser")


logging.info("Checking de l'url du site terminé avec succès...")

logging.info("Récuperation de tous les liens (1000) du site web...")


def get_all_book_links():
    """
    Récupère tous les liens de livres (1000 liens)
    depuis les 50 pages de catalogue.
    """
    book_links = []

    # Il y a 50 pages de catalogue
    for page in range(1, 51):
        if page == 1:
            url = BASE_URL + "catalogue/page-1.html"
        else:
            url = BASE_URL + f"catalogue/page-{page}.html"

        soup = get_soup(url)
        if soup is None:
            continue

        # Tous les liens des livres sur cette page
        books = soup.select("article.product_pod h3 a")

        for b in books:
            link = b["href"].replace("../../", "")
            book_links.append(BASE_URL + "catalogue/" + link)

        print(f"Page {page} récupérée ({len(books)} livres)")

        time.sleep(0.2)  # éviter de spammer le site

    return book_links


logging.info("Récuperation terminés avec succès...")

logging.info("Sauvergarde des liens du site web dans un fichier txt...")


def save_book_links(file_path="../data/raw/book_links.txt"):
    """
    Récupère tous les liens de livres et les sauvegarde dans un fichier texte.

    Args:
        file_path (str): Chemin du fichier où les liens seront sauvegardés.
    """
    # Récupération des liens
    all_links = get_all_book_links()

    # Sauvegarde dans le fichier
    with open(file_path, "w", encoding="utf-8") as f:
        for link in all_links:
            f.write(link + "\n")
        print(f"Total de livres récupérés : {len(all_links)}")
        logging.info(f"Total de livres récupérés : {len(all_links)}")
    print(f"✅ Tous les liens ont été sauvegardés dans {file_path}")


logging.info("Sauvegarde terminés avec succès...")


# Exemple d'utilisation
if __name__ == "__main__":
    save_book_links()

print("Fin du scraping.")
logging.info("Fin du scraping…")

##############################################################################

logging.basicConfig(
    level=logging.DEBUG,                      # Niveau minimal de log
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format du message
    filename="app.log",                       # Fichier de sortie (optionnel)
    filemode="a"                              # 'w' = écraser, 'a' = ajouter
)

logging.info("Stockages des fichiers dans MMinIO...")

# Chargement du fichier .env
logging.info("Chargement des variables d'environnments...")
load_dotenv()

# Récupération des secrets
endpoint = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = os.getenv("MINIO_BUCKET")

logging.info("Chargement des secrets d'environnments terminés avec succès...")

logging.info("Connexion au Client MinIO...")

# Connexion au client MinIO
client = Minio(
    endpoint,
    access_key=access_key,
    secret_key=secret_key,
    secure=False
)

logging.info("Connexion au Client MinIO avec succès...")

# Nom du bucket où sauvegarder
bucket_name = "donnees-brutes"

logging.info("Création du bucket encours...")

# Créer le bucket s'il n'existe pas
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)

logging.info("Création du bucket terminés avec succès...")

logging.info("Chargement des fichiers txt dans le bucket...")

# Uploader un fichier TXT
client.fput_object(
    bucket_name,
    "fichiers/raw/books_links.txt",          # Nom de l’objet dans MinIO
    "data/raw/book_links.txt"                    # Fichier local
)

logging.info("Chargement des fichiers terminés avec succès...")

logging.info("Chargement des fichiers csv dans le bucket...")

# Uploader un fichier CSV
client.fput_object(
    bucket_name,
    "fichiers/raw/books_finals.csv",
    "data/raw/books_final.csv"
)
logging.info("Chargement des fichiers terminés avec succès...")

print("✅ Fichiers sauvegardés dans MinIO")
logging.info("Stockages des fichiers dans MMinIO terminés...")
###########################################################################
