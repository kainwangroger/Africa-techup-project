import requests
from bs4 import BeautifulSoup
import csv


def get_text_or_none(soup_element):
    if soup_element:
        return soup_element.text.strip()
    return None


def main():
    print("Début du scraping…")
    url = ("https://books.toscrape.com/catalogue"
           "/a-light-in-the-attic_1000/index.html")
    response = requests.get(url)

    print("Status code:", response.status_code)
    if response.status_code == 200:
        print("Connexion réussie.")

        f = open("books.html", "w", encoding='utf-8')
        f.write(response.text)
        f.close()
        with open("books.html", "r", encoding="utf-8") as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, "html5lib")
        # Titre de la page
        titre_page = get_text_or_none(soup.find('header', class_='header'))
        print("Titre de la page:", titre_page)
        # Informations du livre
        div_bloc = soup.find('div', class_='page')

        titre_article = get_text_or_none(div_bloc.find('h1'))
        print("Titre du livre:", titre_article)
        quantité_stock = get_text_or_none(div_bloc.find
                                          ('p', class_='instock availability'))
        print("Quantité en stock:", quantité_stock)

        prix_article = get_text_or_none(div_bloc.find
                                        ('p', class_='price_color'))
        print("Prix du livre:", prix_article)

        description_bloc = div_bloc.find('div', id='product_description')
        if description_bloc:
            description_titre = get_text_or_none(description_bloc.h2)
            print("Titre du bloc Description:", description_titre)
            description_article = description_bloc.find_next_sibling('p')
            print("Description du livre:", get_text_or_none(
                                        description_article))
        upc_bloc = div_bloc.find('th', string='UPC')
        if upc_bloc:
            upc_title = get_text_or_none(upc_bloc)
            print("Titre du bloc UPC:", upc_title)
            upc_valeur = upc_bloc.find_next_sibling('td')
            print("UPC du livre:", get_text_or_none(upc_valeur))

        type_produit_bloc = div_bloc.find('th', string='Product Type')
        if type_produit_bloc:
            type_produit_title = get_text_or_none(type_produit_bloc)
            print("Titre du bloc Type de produit:", type_produit_title)
            type_produit_valeur = type_produit_bloc.find_next_sibling('td')
            print("Type de produit:", get_text_or_none(type_produit_valeur))

        prix_hors_taxe_bloc = div_bloc.find('th', string='Price (excl. tax)')
        if prix_hors_taxe_bloc:
            prix_hors_taxe_title = get_text_or_none(prix_hors_taxe_bloc)
            print("Titre du bloc Prix hors taxe:", prix_hors_taxe_title)
            prix_hors_taxe_valeur = prix_hors_taxe_bloc.find_next_sibling('td')
            print("Prix hors taxe:", get_text_or_none(prix_hors_taxe_valeur))

        prix_ttc_bloc = div_bloc.find('th', string='Price (incl. tax)')
        if prix_ttc_bloc:
            prix_ttc_title = get_text_or_none(prix_ttc_bloc)
            print("Titre du bloc Prix TTC:", prix_ttc_title)
            prix_ttc_valeur = prix_ttc_bloc.find_next_sibling('td')
            print("Prix TTC:", get_text_or_none(prix_ttc_valeur))

        disponibilite_bloc = div_bloc.find('th', string='Availability')
        if disponibilite_bloc:
            disponibilite_title = get_text_or_none(disponibilite_bloc)
            print("Titre du bloc Disponibilité:", disponibilite_title)
            disponibilite_valeur = disponibilite_bloc.find_next_sibling('td')
            print("Disponibilité:", get_text_or_none(disponibilite_valeur))

        nombre_davis_bloc = div_bloc.find('th', string='Number of reviews')
        if nombre_davis_bloc:
            nombre_davis_title = get_text_or_none(nombre_davis_bloc)
            print("Titre du bloc Nombre d'avis:", nombre_davis_title)
            nombre_davis_valeur = nombre_davis_bloc.find_next_sibling('td')
            print("Nombre d'avis:", get_text_or_none(nombre_davis_valeur))

        # Écriture dans un fichier CSV
        with open("books.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([titre_article, "prix_total",
                             description_titre,
                             upc_title,
                             type_produit_title,
                             prix_hors_taxe_title,
                             prix_ttc_title,
                             disponibilite_title,
                             nombre_davis_title])
    else:
        print("Échec de la connexion.")

    print("Scraping terminé.")


if __name__ == "__main__":
    main()
