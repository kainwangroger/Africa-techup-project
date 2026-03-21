# 🚀 Projet fil conducteur – Pipeline ETL industrialisé

## 📌 Présentation générale

Ce projet a pour objectif de concevoir et implémenter un **pipeline ETL (Extract, Transform, Load) industrialisé**, couvrant l'ensemble du cycle de vie de la donnée, depuis des **sources hétérogènes** jusqu'à une **architecture Lakehouse moderne**.

Il s'inscrit dans une logique **Data Engineering professionnelle**, avec automatisation, scalabilité, fiabilité, observabilité et bonnes pratiques DevOps.

---

## 🎯 Objectifs du projet

- Automatiser la collecte de données depuis **4 sources hétérogènes**.
- Traiter les données avec **Pandas** (flexibilité) et **Apache Spark** (scalabilité).
- Stocker les données dans un **Data Lake S3 (MinIO)** (Bronze/Silver).
- Centraliser les métriques dans un **Data Warehouse (PostgreSQL)** – schéma `analytics` (Gold).
- Visualiser les données avec **Apache Superset** et **Grafana**.
- Monitorer l'infrastructure et centraliser les logs avec la stack **Prometheus, Loki, Promtail et cAdvisor**.
- Orchestrer l'ensemble avec **Apache Airflow**.

---

## 🧱 Architecture globale

### Vue d'ensemble

```
Sources de données
   │
   ├── Site Web (Scraping – Books to Scrape)
   ├── API REST (RestCountries)
   ├── API World Bank (Population)
   ├── API Exchange Rates (Taux de change GBP)
   │
   ▼
[EXTRACT]
   │
   ▼
[RAW ZONE] (Bronze - MinIO)
   │
   ▼
[TRANSFORM] (Pandas / Apache Spark)
   │
   ▼
[CLEAN ZONE] (Silver - MinIO)
   │
   ▼
[ANALYST ZONE] (Gold - PostgreSQL, schéma analytics)
   │
   ├──▶ [DATA VISUALIZATION] (Apache Superset, Grafana)
   └──▶ [MONITORING & LOGS] (Prometheus, Loki, Promtail, cAdvisor)
```

---

## 📊 Sources de données

### 1️⃣ Web Scraping (Livres)
- **Source** : [Books to Scrape](https://books.toscrape.com/)
- **Détails** : 1000 livres – titres, prix, stocks, descriptions, images.

### 2️⃣ API REST (Pays)
- **Source** : [RestCountries API](https://restcountries.com/v3.1/all)
- **Détails** : ~250 pays – informations géographiques, démographiques, devises.

### 3️⃣ API World Bank (Population)
- **Source** : [World Bank Open Data](https://api.worldbank.org/v2/)
- **Détails** : Données historiques de population mondiale (pagination automatique).

### 4️⃣ API Exchange Rates (Devises)
- **Source** : [ExchangeRate-API](https://api.exchangerate-api.com/v4/latest/GBP)
- **Détails** : Taux de change GBP → USD/EUR/ZAR/NGN/KES/EGP pour la conversion des prix.

---

## 🔄 Pipeline ETL

### 🔹 Extraction

- Scripts Python dédiés par source (7 scripts)
- Gestion des erreurs (try/except)
- Logs structurés via module centralisé

---

### 🔹 Transformation (Pandas & Spark)

Transformations principales :

- Nettoyage des données (prix, disponibilité, textes)
- Suppression des doublons
- Typage des colonnes
- Conversion de devises (GBP → EUR/USD)
- Calcul de densité de population
- Validation métier (détection de valeurs nulles)

Deux moteurs disponibles :
- **Pandas** : pour la flexibilité et les petits volumes
- **Apache Spark** : pour la scalabilité et les grands volumes (avec Delta Lake)

---

### 🔹 Chargement

- **MinIO (S3)** : Bronze (`data/raw/` → `bronze/`) et Silver (`data/silver/` → `silver/`)
- **PostgreSQL** : Gold (schéma `analytics`) via UPSERT avec audit
- Format : **Parquet** (compression snappy)

---

## ⏱️ Orchestration – Apache Airflow

### DAG principal : `africa_techup_unified_pipeline`

```
start → [extract_books, extract_countries, extract_worldbank, extract_rates]
         extract_rates → transform_rates
         [extract_books, transform_rates] → transform_books
         extract_countries → transform_countries
         extract_worldbank → transform_worldbank
         [transform_books, transform_countries] → gold_summary → load_to_postgres
         [all transforms + load_sql] → upload_to_minio → end
```

3 DAGs disponibles :
- `africa_techup_unified_pipeline` – Pandas + Gold + PostgreSQL ⭐
- `books_etl_pandas_pipeline` – Pandas uniquement
- `kainwang_pipeline_etl_books` – Spark + Pandas

---

## 🐳 Services Docker

| Service | Rôle | Port |
|---------|------|------|
| **PostgreSQL** | Base de données (Airflow + Gold) | 5432 |
| **pgAdmin** | Interface PostgreSQL | 5050 |
| **Airflow** | Orchestration des pipelines | 8085 |
| **MinIO** | Object Storage (Data Lake S3) | 9000/8900 |
| **Spark Master** | Moteur de transformation distribué | 8081/7077 |
| **Spark Worker** | Nœud de calcul (2 cores, 2GB) | - |
| **Grafana** | Centralisation des dashboards (Métriques + Logs) | 3001 |
| **Apache Superset** | BI et Visualisation de l'entrepôt de données | 8088 |
| **Prometheus** | Collecteur des métriques d'infrastructure | 9090 |
| **cAdvisor** | Monitoring CPU/RAM des conteneurs Docker | 8082 |
| **Loki** | Base de données d'agrégation des logs | 3100 |
| **Promtail/StatsD/PG Exp** | Agents d'exportation des logs et métriques | Divers |

### 💾 Format de Stockage

- **Bronze/Silver** : Parquet dans MinIO
- **Silver Spark** : Delta Lake (transactions ACID, schéma enforcement)
- **Gold** : PostgreSQL (schéma `analytics`)

---

## 🧪 Tests & Qualité

- **Pytest** : 6 fichiers de tests (`pytest tests/`)
- **CI/CD** : GitHub Actions (lint flake8 + build Docker)
- **Validation** : Contrôles d'intégrité intégrés dans les scripts (détection de valeurs nulles, doublons)

---

## ▶️ Guide de Lancement Rapide

```bash
# Démarrer PostgreSQL
make postgres

# Initialiser Airflow (migration DB + user admin)
make init

# Lancer tous les services
make up
```

### Interfaces :

- Airflow : http://localhost:8085 (admin/admin)
- Apache Superset : http://localhost:8088 (admin/admin)
- Grafana : http://localhost:3001 (admin/admin)
- Prometheus : http://localhost:9090
- MinIO : http://localhost:8900 (minioadmin/minioadmin)
- pgAdmin : http://localhost:5050 (admin@admin.com/admin)
- Spark UI : http://localhost:8081

### Grafana & Monitoring :
- **Prometheus** (Métriques) et **Loki** (Logs) sont auto-provisionnés en sources de données. Vous pouvez explorer les logs de tous les conteneurs (ex: dans l'onglet *Explore* de Grafana).
- Pour créer des tableaux sur les données, la connexion PostgreSQL dans Superset est : `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`.
