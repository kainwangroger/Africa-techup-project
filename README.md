---

## 🧱 Architecture & Dossiers
Pour comprendre le rôle de chaque fichier et naviguer sereinement dans le projet, consultez ma **[Cartographie du Projet (project_overview.md)](./project_overview.md)**.
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

## 🚀 Guide de Démarrage Rapide

### 1. Prérequis

Assurez-vous d'avoir les outils suivants installés :

- **Git**
- **Docker** & **Docker Compose**
- **Python 3.10+** (uniquement pour l'exécution locale hors Docker)

### 2. Installation & Configuration

```bash
# 1. Cloner le projet
git clone https://github.com/roger/Africa-techup-project.git
cd Africa-techup-project

# 2. Configurer les variables d'environnement
cp .env.example .env
# Note : Éditez le fichier .env si vous souhaitez changer les mots de passe par défaut.
```

### 3. Lancement des Services (Docker)

> [!NOTE]  
> Les commandes `make` ci-après sont optimisées pour **Linux / macOS / WSL**.  
> Si vous êtes sur **Windows (PowerShell/CMD)** sans `make`, utilisez les commandes Docker natives indiquées à droite.

| Action | Linux / macOS / WSL | Windows (Natif) |
| :--- | :--- | :--- |
| **1. Base de données** | `make postgres` | `docker compose up -d postgres` |
| **2. Initialisation** | `make init` | `docker compose up -d airflow-init` |
| **3. Lancement Total** | `make up` | `docker compose up -d` |
| **4. Arrêt / Nettoyage** | `make down` | `docker compose down -v` |

### 4. Accès aux Interfaces

| Service | URL | Identifiants |
| :--- | :--- | :--- |
| **Airflow** | [http://localhost:8085](http://localhost:8085) | `admin` / `admin` |
| **MinIO (S3)** | [http://localhost:8900](http://localhost:8900) | `minioadmin` / `minioadmin` |
| **Grafana** | [http://localhost:3001](http://localhost:3001) | `admin` / `admin` |
| **Superset** | [http://localhost:8088](http://localhost:8088) | `admin` / `admin` |
| **pgAdmin** | [http://localhost:5050](http://localhost:5050) | `admin@admin.com` / `admin` |

---

## 💎 Argumentaire (Mon Pitch)

Dans le cadre de cette formation AfricaTechUp, j'ai souhaité livrer un projet qui ne se contente pas de déplacer de la donnée, mais qui la sécurise et la valorise. Mon pipeline respecte la méthodologie **Medallion**, assure une traçabilité totale via les logs centralisés (**Loki**) et offre une visibilité immédiate sur les performances via **Grafana**. C'est un moteur ETL complet, scalable et prêt pour la production.
met de surveiller dynamiquement :

- La connectivité des bases de données (**Heartbeat**).
- Le débit des entrées/sorties de **MinIO**.
- Les files d'attente et transactions **PostgreSQL**.

---

## 📊 Informations Importantes (Architecture Multi-DB)

Pour garantir la fiabilité, le projet utilise deux bases de données distinctes dans PostgreSQL :

1. **`airflow`** : Contient les données d'Airflow et nos tables finales (schéma `analytics`).
2. **`superset_db`** : Dédiée aux métadonnées internes de Superset pour éviter tout conflit.

> [!TIP]  
> Dans **Superset**, pour créer nos dashboards, connectez-vous à la base **`airflow`** avec l'URI suivante :  
> `postgresql+psycopg2://airflow:airflow@postgres:5432/airflow`

> [!NOTE]  
> Pour la gestion des bases, utilisez **pgAdmin** (`http://localhost:5050`). Le serveur Postgres y est accessible via le host `postgres` avec les identifiants `airflow` / `airflow`.
