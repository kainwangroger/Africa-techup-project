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
[ANALYTICS ZONE] (Gold - MinIO + PostgreSQL, schéma analytics)
   │
   ├──▶ [DATA VISUALIZATION] (Apache Superset, Grafana)
   └──▶ [MONITORING & LOGS] (Prometheus, Loki, Promtail, cAdvisor)
```

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

---

## ▶️ Guide de Lancement Rapide

```bash
# Lancer tous les services
docker compose up -d
```

### Interfaces :
- **Airflow** : `http://localhost:8085` (admin/admin)
- **Apache Superset** : `http://localhost:8088` (admin/admin) — *Données métier*
- **Grafana** : `http://localhost:3001` (admin/admin) — *Observabilité système & app*
- **MinIO** : `http://localhost:8900` (minioadmin/minioadmin) — *Data Lake*
- **pgAdmin** : `http://localhost:5050` (admin@admin.com/admin) — *Gestion BDD*

---

## 💎 Le Pitch : Pourquoi ce projet est "Expert" ?

> *"Imaginez un système capable non seulement de collecter des milliers de données hétérogènes chaque jour, mais aussi de s'auto-surveiller en temps réel."*

Ce projet démontre la maîtrise d'une stack **Data Engineering moderne** :
1.  **Ingestion Hybride** : Scraping HTML + APIs REST.
2.  **Architecture Medallion** : Rigueur du stockage (Bronze/Silver/Gold) pour une qualité de donnée irréprochable.
3.  **Scalabilité Spark** : Prêt pour le Big Data.
4.  **Observabilité 360°** : Grâce à **Prometheus & Grafana**, on ne devine pas que le pipeline fonctionne, on le **prouve**. 
5.  **Industrialisation** : Tout est packagé dans Docker. Un seul `docker compose up` et l'entrepôt de données est vivant.

---

## 📈 Guide des Tableaux de Bord

### 📊 Apache Superset (BI - Métier)
Utilisez le guide [Dashboards_User_Guide.md](./Dashboards_User_Guide.md) pour créer vos graphiques en 2 minutes grâce à la **"Master View"** SQL.

### 🛡️ Grafana (Observabilité - Technique)
**Zero-Touch Configuration** : Le dashboard est déjà importé ! Connectez-vous à Grafana et cherchez le tableau de bord nommé :  
👉 **`🚀 Africa TechUp - FINAL STABLE MONITOR`**.

Il vous permet de voir :
- Si la base de données est vivante (**Heartbeat**).
- Le volume de données qui rentre dans **MinIO**.
- Les **Transactions PostgreSQL** qui s'affolent quand le pipeline Airflow tourne.

---

## 📊 Informations Importantes (Architecture Multi-DB)

Pour garantir la fiabilité, le projet utilise deux bases de données distinctes dans PostgreSQL :
1. **`airflow`** : Contient les données d'Airflow et vos tables finales (schéma `analytics`).
2. **`superset_db`** : Dédiée aux métadonnées internes de Superset pour éviter tout conflit.

> [!TIP]  
> Dans **Superset**, pour créer vos dashboards, connectez-vous à la base **`airflow`** avec l'URI suivante :  
> `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`

> [!NOTE]  
> Pour la gestion des bases, utilisez **pgAdmin** (`http://localhost:5050`). Le serveur Postgres y est accessible via le host `postgres` avec les identifiants `airflow` / `airflow`.
