# 📊 Guide Complet des Dashboards (Superset & Grafana)

Ce guide récapitule les **3 tables clés** que vous avez dans PostgreSQL et comment les utiliser pour impressionner le jury.

---

## 🚀 1. Les 3 Tables Disponibles dans Superset
Une fois connecté à la base `airflow` (schéma `analytics`), vous verrez :
1. **`gold_books_details`** : L'intégralité des 1000 livres (titres, prix GBP/EUR/USD, stock, avis).
2. **`gold_project_summary`** : La synthèse "Big Data" (Total Livres, Total Pays, Population Mondiale, Taux de change).
3. **`gold_market_intelligence`** : La table experte (Jointure Livres x Pays x Devises) pour simuler les prix locaux.

---

## 📊 2. Plan du Dashboard (Le "Blueprint" des 8 Graphiques)

Pour un résultat professionnel, vous allez créer **3 Datasets** (un par table) et **8 Graphiques** répartis dans des **onglets**.

### 📂 Onglet 1 : Marché Mondial
*Dataset : `gold_market_intelligence`*
1. **Big Number** : Prix Moyen (Local) -> Metric: `AVG(price_local)`.
2. **Tableau Intelligence** : `country`, `book_title`, `price_local`.
3. **Bar Chart** : Prix par Pays -> X: `country`, Metric: `AVG(price_local)`.

### 📂 Onglet 2 : Analyse des Livres (Visuels Avancés)
*Dataset : `gold_books_details`*
4. **TreeMap** : Répartition Hiérarchique -> Group by: `category`, Metric: `COUNT(*)`. 
   - *Pourquoi ?* C'est plus moderne et lisible qu'un Pie Chart pour 50 catégories.
5. **Box Plot** : Distribution des Prix -> X: `category`, Metric: `price_eur`.
   - *Effet Waoo* : Ce graphe montre le min, le max et la moyenne des prix par genre. Très "Data Science" !
6. **Time Series (Ligne)** : Évolution de l'Ingestion -> Time: `processed_at`, Metric: `COUNT(*)`.
   - *Pourquoi ?* Pour montrer le flux de données dans le temps.

### 📂 Onglet 3 : État du Système (4 Sources)
*Dataset : `gold_project_summary`*
7. **Big Number with Trendline** : Volume de livres.
8. **Gauge Chart** : Capacité Ingestion (ex: 0 à 1000 livres).

---

## 🛠️ 3. Comment créer les Onglets et Filtres ?

1. **Onglets** : Dans l'édition du Dashboard, cherchez **"Tabs"** dans la barre latérale "Layout Elements". Glissez-le sur votre page. Cliquez sur "Tab 1" pour le renommer.
2. **Filtres** : Cliquez sur l'icône **"Filters"** à gauche (ou glissez un élément "Filter Box"). 
   - Choisissez le Dataset `gold_market_intelligence`.
   - Colonne : `region` ou `country`.
   - *Magie* : Quand vous filtrerez sur "Europe", tous les graphes de l'onglet Marché se mettront à jour !

---

## 🧭 4. Besoin de SQL ? (Mode Expert)
Les données dans Superset proviennent de la base PostgreSQL. 
- **Comment les rafraîchir ?** Lancez le DAG `africa_techup_unified_pipeline` dans Airflow. 
- **Note** : Le pipeline est maintenant intelligent et ne télécharge pas les données si elles sont déjà présentes et récentes (Incrémental).

---

## 📈 5. Grafana (Observabilité)
- **URL** : `http://localhost:3001`
- **CPU/RAM** : Utilisez la source Prometheus.
- **LOGS** : Utilisez la source Loki pour voir les traces d'Airflow et Spark.
