# 🌍 Projet Africa TechUp

## 🧱 Architecture & Dossiers
Le projet est une plateforme de données industrialisée utilisant une architecture **Medallion** (Bronze/Silver/Gold).

- **Orchestrateur** : Apache Airflow (gère le planning et l'ordre des tâches).
- **Traitement** : Pandas & Spark (nettoient et transforment les données).
- **Stockage Data Lake** : MinIO (S3 compatible, couches Bronze/Silver).
- **Stockage Gold** : PostgreSQL (schéma `analytics`).
- **Visualisation Data** : Apache Superset (Data Viz et BI) et Grafana (Dashboards temps réel).
- **Observabilité & Logs** : Prometheus, Loki, Promtail, cAdvisor (Monitoring complet Docker, Airflow, Spark).
- **Infrastructure** : Docker Compose (lance plus de 13 services automatisés).
- **CI/CD** : GitHub Actions (lint flake8 + build Docker).

---

## 📂 Organisation des Fichiers

### 1. `dags/` (L'Orchestration)
Contient les fichiers qui définissent les pipelines Airflow :
- **[unified_africa_techup_dag.py](file:///c:/Users/roger/Desktop/Africa-techup-project/dags/unified_africa_techup_dag.py)** (Le favori ⭐) : Gère 4 sources de données en parallèle avec couche Gold + PostgreSQL.
- **[airflow_books_dags_with_pandas.py](file:///c:/Users/roger/Desktop/Africa-techup-project/dags/airflow_books_dags_with_pandas.py)** : Version Pandas gérant les 4 sources.
- **[airflow_books_dags_with_spark.py](file:///c:/Users/roger/Desktop/Africa-techup-project/dags/airflow_books_dags_with_spark.py)** : Version Spark gérant les 4 sources.

### 2. `include/` (La Logique Métier)
C'est ici que se trouve le code Python :
- **`extract/`** : Scripts d'extraction (Scraping livres, APIs pays/population/taux de change, Kaggle).
- **`transform/`** : Scripts de transformation Pandas et Spark (nettoyage, conversion devises, agrégation Gold).
- **`load/`** : Chargement vers MinIO (S3) et PostgreSQL (schéma `analytics`).
- **`utils/`** : Logging centralisé (`custom_logging.py`).
- **`main_pipeline.py`** : Pipeline exécutable en local (hors Airflow).

### 3. `tests/` (Les Tests)
6 fichiers de tests Pytest : extraction, transformation, conversion devises, Spark, pays, population.

### 4. `.github/workflows/` (Le CI/CD)
- **`ci_cd.yml`** : Automatise lint (flake8) et build Docker à chaque commit.

### 5. Fichiers de Configuration et Infrastructure
- **`docker-compose.yaml`** : Provisionne tous les services (Airflow, MinIO, Spark, Postgres, Superset, Prometheus, Grafana, Loki, etc...).
- **`prometheus/`**, **`promtail/`** & **`grafana/`** : Configuration complète de la stack d'observabilité (Scraping, Logs et Provisioning de dashboards).
- **`postgres/`** : Scripts d'initialisation de la base de données (`init-db.sh`).
- **`scripts-sql.sql`** : Définition des tables finales (Gold) dans PostgreSQL.
- **`prepare_submission.ps1`** : Script automatique de création de l'archive de soumission.
- **`Dashboards_User_Guide.md`** : Guide pas-à-pas pour la création des visuels Superset.

---

## 🌊 Flux de Données (ETL)

### Sources (4)
| Source | Type | Données |
|--------|------|---------|
| Books to Scrape | Web Scraping | 1000 livres (titres, prix, stocks) |
| RestCountries | API REST | ~250 pays (population, superficie, devises) |
| World Bank | API REST | Population mondiale historique |
| ExchangeRate-API | API REST | Taux de change GBP → USD/EUR/ZAR/NGN/KES/EGP |

### Pipeline
1. **Extract** : Récupération des 4 sources → `data/raw/` (Bronze)
2. **Transform** : Nettoyage, conversion de devises GBP→EUR/USD → `data/silver/` (Silver)
3. **Gold** : Agrégation des métriques → `data/gold/` → PostgreSQL (schéma `analytics`)
4. **Load** : Upload Bronze/Silver vers MinIO, Gold vers PostgreSQL
5. **Data Visualization** : Apache Superset connecté à PostgreSQL.
6. **Platform Monitoring** : Prometheus & Loki (via Grafana) surveillant tous ces flux.

---

## 📦 MinIO : Qu'est-ce qui est stocké ?

### 🪵 Couche `bronze/` (Données Brutes)
- `bronze/books_raw_unique.csv`
- `bronze/countries/data_countries.parquet`
- `bronze/worldbank/population_all.parquet`
- `bronze/exchange_rates/rates_gbp.parquet`

### 🥈 Couche `silver/` (Données Propres)
- `silver/books_clean.csv` : Prix convertis en EUR/USD.
- `silver/countries/countries_silver.parquet` : Pays avec densités calculées.
- `silver/worldbank/population_silver_*.parquet` : Population filtrée et versionée.
- `silver/exchange_rates/rates_silver.parquet` : Taux de change enrichis.

### 🏆 Couche Gold (PostgreSQL uniquement)
- `analytics.gold_project_summary` : Métriques agrégées (Business Intelligence).
- `analytics.gold_books_details` : Détails des livres pour les dashboards Superset.
- `analytics.gold_market_intelligence` : Table d'expansion (Prix locaux et simulations).

> **Note** : Le dossier `data/gold/` n'est **pas** uploadé dans MinIO. Il est chargé directement dans PostgreSQL (schéma `analytics`).

---

## ▶️ Deux Modes d'Exécution

Le projet peut être exécuté de **deux façons** :

### Mode 1 : Local (sans Docker ni Airflow)

```bash
python -m include.main_pipeline
```

Le fichier `include/main_pipeline.py` exécute le pipeline complet en séquentiel :
1. **Extract** (5 sources : books, countries, worldbank, exchange rates, kaggle)
2. **Transform** (5 transforms Pandas + conversion de devises)
3. **Gold** (agrégation des métriques)
4. **Load PostgreSQL** (population + gold → schéma `analytics`)
5. **Upload MinIO** (bronze + silver, mode `from_docker=False`)

> ⚠️ Nécessite MinIO et PostgreSQL accessibles en local (ports 9000 et 5432).

### Mode 2 : Airflow (via Docker Compose - Recommandé)

C'est le mode complet qui lance les 13 services (Monitoring, BI, S3, etc.).

> [!NOTE]  
> Les commandes `make` sont pour **Linux/macOS/WSL**. Pour **Windows natif**, utilisez les équivalents `docker compose`.

| Action | Linux / macOS / WSL | Windows (Natif) |
| :--- | :--- | :--- |
| **1. Config** | `cp .env.example .env` | `copy .env.example .env` |
| **2. DB** | `make postgres` | `docker compose up -d postgres` |
| **3. Init** | `make init` | `docker compose up -d airflow-init` |
| **4. Start** | `make up` | `docker compose up -d` |

Puis ouvrir Airflow ([http://localhost:8085](http://localhost:8085)) et lancer le DAG `africa_techup_unified_pipeline` (Planifié toutes les 4 heures).

---

## 🐳 Services Docker

| Service | URL | Identifiants |
|---------|-----|-------------|
| **Airflow** | http://localhost:8085 | `admin` / `admin` |
| **Apache Superset** | http://localhost:8088 | `admin` / `admin` |
| **Grafana** | http://localhost:3001 | `admin` / `admin` |
| **Prometheus** | http://localhost:9090 | N/A |
| **MinIO** | http://localhost:8900 | `minioadmin` / `minioadmin` |
| **pgAdmin** | http://localhost:5050 | `admin@admin.com` / `admin` |
| **Spark UI** | [http://localhost:8081](http://localhost:8081) | N/A |

---

## 📊 Visualisation & Observabilité

### 1. Grafana (Observabilité 360°)
C'est ici que nous surveillons la santé technique de toute l'infrastructure.
- **Les moteurs (Backend)** :
  - **Promtail / Loki** : Collecte et indexage des logs conteneurs.
  - **cAdvisor** : État hardware (CPU, RAM) et uptime des conteneurs Docker en temps réel.
  - **Prometheus** : Stockage des métriques de performance récoltées.
- **L'interface (Frontend)** : Dashboard complet incluant 15 indicateurs (Transactions, Volume S3, Santé des clusters Spark, et Ressources Hôte).

### 2. Apache Superset (Business Intelligence)

- Création automatique de l'admin à l'initialisation.
- **Base de données isolée** : Superset utilise `superset_db` pour ses réglages internes.
- **Accès aux données** : Utiliser l'URI `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow` pour exposer la couche Gold (`analytics`).

---

## 🧪 Tests

```bash
pytest tests/
```

6 fichiers de tests couvrant : extraction, transformation, conversion de devises, Spark, pays, population.

---

## 🔄 CI/CD

GitHub Actions (`.github/workflows/ci_cd.yml`) :
- **Lint** : `flake8` (erreurs syntaxiques + warnings)
- **Build** : Construction de l'image Docker Airflow
- Déclenché sur push/PR vers `main` et `dev`
