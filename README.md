---

## 🧱 Architecture & Dossiers
Pour comprendre le rôle de chaque fichier et naviguer sereinement dans le projet, consultez ma **[Cartographie du Projet (PROJECT_STRUCTURE.md)](./PROJECT_STRUCTURE.md)**.
Le projet mobilise les outils phares du parcours :
- **Orchestration** : Apache Airflow.
- **Traitement Distribué** : Apache Spark & Pandas.
- **Stockage Objets** : MinIO (Architecture Medallion : Bronze / Silver).
- **Entrepôt de Données** : PostgreSQL (Couche Gold).

---

## 🚀 Extensions & Valeur Ajoutée (Bonus)
Pour aller au-delà des objectifs de base et garantir une exploitation professionnelle, j'ai ajouté deux couches supplémentaires :

### 1. Observabilité & Monitoring (Stack PLG)
Maîtrise de la santé du pipeline via **Prometheus**, **Loki** et **Grafana**. 
- Le dashboard **`🚀 Africa TechUp - FINAL STABLE MONITOR`** est pré-configuré pour surveiller les transactions et la disponibilité des services (Grafana Zero-Touch).

### 2. Business Intelligence (BI)
Visualisation des données métiers via **Apache Superset** pour transformer les tables Gold en insights actionnables.

---

## 🐳 Guide de Lancement
```bash
# Lancement de l'ensemble de l'écosystème
docker compose up -d
```

### Accès aux outils :
- **Airflow** : `http://localhost:8085` (admin/admin)
- **MinIO** : `http://localhost:8900` (minioadmin/minioadmin)
- **PostgreSQL** : `localhost:5432` (via pgAdmin sur le port 5050)
- **Superset** : `http://localhost:8088` (admin/admin) — *Extension BI*
- **Grafana** : `http://localhost:3001` (admin/admin) — *Extension Monitoring*

---

## 💎 Argumentaire de Soutenance (Mon Pitch)
Dans le cadre de cette formation AfricaTechUp, j'ai souhaité livrer un projet qui ne se contente pas de déplacer de la donnée, mais qui la sécurise et la valorise. Mon pipeline respecte la méthodologie **Medallion**, assure une traçabilité totale via les logs centralisés (**Loki**) et offre une visibilité immédiate sur les performances via **Grafana**. C'est un moteur ETL complet, scalable et prêt pour la production.
met de surveiller dynamiquement :
- La connectivité des bases de données (**Heartbeat**).
- Le débit des entrées/sorties de **MinIO**.
- Les files d'attente et transactions **PostgreSQL**.

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
