# 🚀 Projet fil conducteur – Pipeline ETL industrialisé

## 📌 Présentation générale

Ce projet a pour objectif de concevoir et implémenter un **pipeline ETL (Extract, Transform, Load) industrialisé**, couvrant l’ensemble du cycle de vie de la donnée, depuis des **sources hétérogènes** jusqu’à une **architecture Lakehouse moderne**.

Il s’inscrit dans une logique **Data Engineering professionnelle**, avec automatisation, scalabilité, fiabilité, observabilité et bonnes pratiques DevOps.

---

## 🎯 Objectifs du projet

- Automatiser la collecte de données depuis plusieurs sources
- Traiter les données via des transformations distribuées avec **Apache Spark**
- Stocker les données dans un **Object Storage S3-compatible (MinIO)**
- Orchestrer l’ensemble du pipeline avec **Apache Airflow**
- Industrialiser la solution (Docker, tests, logs, monitoring)

---

## 🧱 Architecture globale

### Vue d’ensemble

```
Sources de données
   │
   ├── Site Web (Scraping)
   ├── Fichier CSV
   ├── Base de données SQL
   │
   ▼
[EXTRACT]
   │
   ▼
[RAW ZONE]  (MinIO)
   │
   ▼
[TRANSFORM] (Apache Spark)
   │
   ▼
[CURATED ZONE] (MinIO partitionné)
   │
   ▼
[Monitoring & Logs]
```

---

## 📊 Sources de données

### 1️⃣ Site Web – Web Scraping
- **URL** : https://books.toscrape.com/
- **Technologies** : `requests`, `BeautifulSoup`
- **Données extraites** :
  - Titre
  - Prix
  - Disponibilité
  - Catégorie
  - Rating

📁 Stockage brut : `s3://raw/books/`

---

### 2️⃣ Fichier CSV
- Source : Google Drive (téléchargement automatisé)
- Traitements initiaux :
  - Validation du schéma
  - Nettoyage des valeurs nulles
  - Conversion des types

📁 Stockage brut : `s3://raw/csv_data/`

---

### 3️⃣ Base de données SQL
- Exemple : PostgreSQL
- Connexion via JDBC (Spark)
- Cas d’usage : données transactionnelles

📁 Stockage brut : `s3://raw/sql_data/`

---

## 🔄 Pipeline ETL

### 🔹 Extraction

- Scripts Python dédiés par source
- Gestion des erreurs (try/except)
- Logs structurés (JSON)

---

### 🔹 Transformation (Apache Spark)

Transformations principales :
- Nettoyage des données
- Suppression des doublons
- Typage des colonnes
- Validation métier

Pourquoi Spark ?
- Scalabilité
- Traitements distribués
- Robustesse sur grands volumes

---

### 🔹 Chargement

- Destination : **MinIO (S3 compatible)**
- Format : **Parquet**
- Partitionnement :

```
year=YYYY/month=MM
```

Exemple :
```
s3://curated/books/year=2025/month=01/
```

---

## ⏱️ Orchestration – Apache Airflow

### DAG principal

```
extract_web
extract_csv
extract_sql
      │
      ▼
transform_spark
      │
      ▼
load_minio
```

Fonctionnalités :
- Planification automatique
- Reprise sur échec (retries)
- Monitoring via Airflow UI

---

## 🐳 Industrialisation

### Docker & Conteneurisation

Services inclus :
- Apache Airflow
- Apache Spark
- MinIO
- PostgreSQL

✔ Déploiement via `docker-compose`
✔ Environnements isolés et reproductibles

---

### Configuration & Secrets

- Variables d’environnement (`.env`)
- Variables Airflow
- Aucun secret en dur dans le code

---

## 🧪 Tests & Qualité

- Framework : `pytest`
- Tests unitaires :
  - Extraction
  - Transformation
- Couverture de tests > **80%**

---

## 📈 Logging & Monitoring

### Logging
- Format : JSON
- Niveaux : INFO / WARNING / ERROR

### Monitoring
- Airflow UI
- Spark UI
- Métriques :
  - Temps d’exécution
  - Volume de données
  - Échecs

---

## ✅ Critères de validation

### Fonctionnels
- ✔ 3 sources de données
- ✔ Transformations distribuées
- ✔ Stockage partitionné
- ✔ Orchestration automatisée
- ✔ Reprise sur erreur

### Techniques
- ✔ Git
- ✔ Docker
- ✔ Tests > 80%
- ✔ Logs structurés
- ✔ Monitoring

### Qualité
- ✔ Documentation complète
- ✔ Code lisible et structuré
- ✔ Sécurité des secrets
- ✔ Déploiement automatisé

---

## 📦 Structure du dépôt

```
etl-pipeline/
 ├── dags/
 ├── extract/
 ├── spark/
 ├── load/
 ├── tests/
 ├── docker/
 ├── docs/
 ├── docker-compose.yml
 └── README.md
```

---

## ▶️ Lancement du projet

```bash
docker-compose up -d
```

Accès aux interfaces :
- Airflow : http://localhost:8080
- MinIO : http://localhost:9001
- Spark UI : http://localhost:4040

---

## 🎥 Démonstration attendue

- Exécution complète du pipeline
- Visualisation des DAGs Airflow
- Données présentes dans MinIO
- Présentation des métriques de performance

---

## 🏁 Conclusion

Ce projet démontre la capacité à concevoir un **pipeline ETL robuste, scalable et industrialisé**, conforme aux standards professionnels du **Data Engineering moderne**.

