# 🗺️ Cartographie du Projet AfricaTechUp (Pipeline ETL)

Ce document explique le rôle de chaque dossier et fichier pour vous aider à naviguer dans l'architecture du projet.

---

## 🏗️ 1. Cœur du Pipeline (ETL & Orchestration)

- **`dags/`** : Contient le "cerveau" du projet (`africa_techup_unified_pipeline.py`). Ce fichier définit l'ordre d'exécution de toutes les tâches (Scraping -> Transformation -> Chargement).
- **`include/`** : Contient les scripts Python "travailleurs" :
  - `extract/` : Scripts de collecte de données (Scraping des livres, APIs Population/Taux de change).
  - `transform/` : Logique de nettoyage des données avec **Pandas** et **Apache Spark**.
  - `load/` : Scripts de chargement vers **MinIO** et **PostgreSQL**.

---

## 🗄️ 2. Stockage de la Donnée (Architecture Medallion)

- **`data/`** : Votre Data Lake local :
  - `bronze/` : Données brutes, telles qu'elles arrivent de l'extérieur.
  - `silver/` : Données nettoyées, standardisées et converties (ex: prix en EUR/USD).
  - `gold/` : Données finales, prêtes pour l'analyse (agrégations).

---

## 🛡️ 3. Observabilité & Monitoring (Bonus Expert)

- **`grafana/`** : Configuration visuelle. Contient le dossier `dashboards/` où se trouve le tableau de bord auto-visionné.
- **`prometheus/`** : Configuration du collecteur de métriques (quand et comment interroger les services).
- **`promtail/`** : Configuration du collecteur de logs (Loki).
- **`postgres/`** : Scripts d'initialisation (`init-db.sh`) pour créer automatiquement les bases de données au démarrage.

---

## ⚙️ 4. Infrastructure & Environnement

- **`docker-compose.yaml`** : Le fichier chef d'orchestre qui lance les 12 services (Airflow, Spark, MinIO, etc.).
- **`.env`** : Contient vos secrets et mots de passe (à ne pas partager publiquement).
- **`Dockerfile`** : La recette de fabrication de votre image Airflow personnalisée (avec Java pour Spark).
- **`requirements.txt`** : La liste de toutes les bibliothèques Python nécessaires au projet.

---

## 🧪 5. Qualité & Utilitaires

- **`tests/`** : Contient vos tests unitaires (**Pytest**) pour vérifier que le code de transformation fonctionne sans erreurs.
- **`prepare_submission.ps1`** : Script automatique pour créer l'archive de soumission finale (ZIP).
