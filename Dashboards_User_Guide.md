# Ce document détaille la démarche pour mettre en place les tableaux de bord décisionnels dans Apache Superset

L'objectif est de transformer nos données transformées (couche Gold) en insights exploitables pour impressionner le jury.

---

## 🚀 1. Connexion Superset & Base de Données

**URL Superset** : `http://localhost:8088`
**Identifiants** : `admin` / `admin`

Avant de créer vos tableaux de bord, vous devez connecter Superset à la base de données où se trouvent vos données Gold :
1. Dans Superset, allez dans **Settings** (en haut à droite) -> **Database Connections**.
2. Cliquez sur **+ Database** -> **PostgreSQL**.
3. Utilisez cette URI de connexion exacte :
   ```text
   postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
   ```
4. Cliquez sur **Connect** puis **Finish**.

Ensuite, allez dans **Datasets** -> **+ Dataset**, choisissez le schéma `analytics`, et vous y trouverez vos 3 tables :

1. **`gold_books_details`** : L'intégralité des 1000 livres (titres, prix GBP/EUR/USD, stock, avis).
2. **`gold_project_summary`** : La synthèse "Big Data" (Total Livres, Total Pays, Population Mondiale, Taux de change).
3. **`gold_market_intelligence`** : La table experte (Jointure Livres x Pays x Devises) pour simuler les prix locaux.

---

## 🏗️ 2. Créer le Dashboard Final & les Onglets

Pour que votre présentation soit organisée, suivez ces étapes :

1. **Créer le Dashboard** : Allez dans **Dashboards** -> **+ DASHBOARD** (en haut à droite).
2. **Activer l'Édition** : Cliquez sur **EDIT DASHBOARD** (crayon en haut à droite).
3. **Ajouter des Onglets** :
   - Sur le panneau de droite, allez dans **Layout Elements**.
   - Glissez-déposez l'élément **Tabs** tout en haut de votre dashboard.
   - Cliquez sur "New Tab" pour les renommer (Catalogue, Géo-Données, Devises).
4. **Placer vos Graphiques** : Glissez vos graphiques créés précédemment dans les onglets correspondants.

---

## 🚀 3. Détails des Onglets Recommandés

### 📂 Onglet 1 : Catalogue de Livres (Scraping)

*Dataset : `gold_books_details`*

1. **Nom : `[LIVRES] Répartition par Catégorie`**
   - Type : `TreeMap` | Group by: `category` | Metric: `COUNT(*)`
2. **Nom : `[LIVRES] Top 10 des livres les plus chers`**
   - Type : `Bar Chart` | X: `title` | Metric: `MAX(price_gbp)`
3. **Nom : `[LIVRES] Distribution des prix par genre`**
   - Type : `Box Plot` | X: `category` | Metric: `price_gbp` (Détails)
4. **Nom : `[LIVRES] Stock Total de la Librairie`**
   - Type : `Big Number` | Metric: `SUM(stock)`

### 📂 Onglet 2 : Géo-Données (Countries API)

*Dataset : `gold_project_summary`*
5. **Nom : `[MONDE] Répartition des Pays par Région`**
   - Type : `Pie Chart` | Dimension: `region` | Metric: `COUNT(*)`

6. **Nom : `[MONDE] Population Mondiale Totale`**
   - Type : `Big Number` | Metric: `SUM(total_population)`

### 📂 Onglet 3 : Taux de Change (Rates API)

*Dataset : `gold_project_summary`*

7. **Nom : `[DEVISES] Taux de conversion vs GBP`**
   - Type : `Bar Chart` | X: `currency_code` | Metric: `AVG(rate)`

### 📂 Onglet 4 : Simulation Expert (Optionnel)

*Dataset : `gold_market_intelligence`*

8. **Nom : `[SIMU] Prix locaux pour l'expansion mondiale`**
   - Type : `Table` | Columns: `country`, `book_title`, `price_local`

9. **Nom : `[SIMU] Prix moyen simulé par Pays`**
   - Type : `Bar Chart` | X: `country` | Metric: `AVG(price_local)`

---

## 🧭 4. Mode Expert : Les Requêtes SQL Lab

Si vous préférez coder vos graphiques en SQL, voici les requêtes à copier-coller dans **SQL Lab** :

| Graphe | Requête SQL (Schéma: `analytics`) |
| :--- | :--- |
| **TreeMap Catégories** | `SELECT category, count(*) FROM analytics.gold_books_details GROUP BY 1;` |
| **Top 10 Livres** | `SELECT title, price_gbp FROM analytics.gold_books_details ORDER BY 2 DESC LIMIT 10;` |
| **Stock Total** | `SELECT sum(stock) FROM analytics.gold_books_details;` |
| **Pop. Mondiale** | `SELECT sum(total_population) FROM analytics.gold_project_summary;` |
| **Taux vs GBP** | `SELECT currency_code, rate FROM analytics.gold_project_summary;` |
| **Simulation** | `SELECT country, book_title, price_local FROM analytics.gold_market_intelligence;` |

---

## 📈 5. Grafana (Observabilité Système)

**URL** : `http://localhost:3001` (admin/admin)

### A. Panneau "Santé des Conteneurs" (Source: Prometheus)

1. **CPU par Service** :
   - Query : `sum(rate(container_cpu_usage_seconds_total{name=~".+"}[5m])) by (name)`
   - Visualisation : **Time Series**
2. **Mémoire par Service** :
   - Query : `container_memory_usage_bytes{name=~".+"}`
   - Visualisation : **Bar Gauge** (pour voir quel service consomme le plus).

### B. Importation Rapide (Mode Automatique)

Si vous ne voulez pas créer les panneaux à la main, j'ai généré un fichier pour vous : **`grafana_dashboard_africa_techup.json`**.

1. Ouvrez Grafana (`localhost:3001`).
2. Allez dans **Dashboards** -> **New** -> **Import**.
3. Cliquez sur **Upload JSON file** et choisissez le fichier à la racine de votre projet.
4. Sélectionnez les DataSources (**Prometheus** et **Loki**) quand on vous le demande.
5. **BOOM !** Votre monitoring de luxe est prêt. 🚀

---

---

## ⚡ 7. Speed-Run : Méthode Optimisée (Master View)

Pour accélérer la création des graphiques, j'ai conçu une **Vue Maître** en SQL qui regroupe toutes les informations utiles.
Au lieu de créer 3 datasets, je vous conseille de créer **un seul Dataset Virtuel** dans **SQL Lab** avec cette requête qui joint tout (les livres et les résumés) :

```sql
SELECT 
    b.*, 
    s.total_population, 
    s.total_countries,
    s.processed_at as sync_date
FROM analytics.gold_books_details b
CROSS JOIN (SELECT * FROM analytics.gold_project_summary LIMIT 1) s;
```
---

## 🛠️ 8. Maintenance & Sécurité (AUTO-RÉPARATION)

Si vous utilisez la commande **`docker compose down -v`**, vous perdez toutes vos données (volumes MinIO et bases SQL). Le projet est désormais conçu pour s'auto-réparer au prochain démarrage.

### 🔄 Procédure de relance "One-Click" :
1.  **Démarrer tout** : `docker compose up -d`
2.  **Attendez 1 minute** : 
    - Le service `init-folders` crée automatiquement l'arborescence `data/` avec les bonnes permissions.
    - Le service `superset-init` recrée l'utilisateur admin s'il a disparu.
3.  **Lancer le DAG** : Allez sur `localhost:8080` et lancez **`unified_africa_techup_dag`**. Il va tout reconstruire de A à Z (Scraping -> Transformation -> Airflow -> MinIO -> Postgres).

### 💡 Conseil d'expert :

- Utilisez simplement **`docker compose down`** (sans le `-v`) pour garder vos données entre deux sessions !
- Si vous voulez forcer un nouveau scraping sans tout supprimer, videz juste le dossier `data/` (mais gardez le dossier lui-même) et relancez le DAG.

Enregistrez cette requête comme un dataset nommé **`MASTER_PROJECT_VIEW`**. Vous aurez toutes les colonnes au même endroit pour créer vos onglets 1, 2 et 3 sans changer de dataset !

### Option B : L'Importation (Avancé)

Superset permet d'importer des fichiers `.zip` ou `.yaml`.
*Attention :* Cela nécessite que les UUID de vos bases de données correspondent. Le plus sûr reste la **Master View** ci-dessus qui réduit le travail manuel à quelques clics.

### Option C : Un seul graphique, 10 copies

N'oubliez pas que vous pouvez **"Save As"** (Enregistrer sous) un graphique existant.

- Créez un premier Bar Chart parfait.
- Faites "Save As" -> Nommez-le différemment.
- Changez juste la métrique ou la colonne.
- C'est 4x plus rapide !
