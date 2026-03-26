# Guide des Tableaux de Bord — Apache Superset

Ce document détaille la démarche pour créer les tableaux de bord décisionnels dans Apache Superset.

---

## 🚀 1. Connexion Superset & Base de Données

**URL Superset** : `http://localhost:8088`
**Identifiants** : `admin` / `admin`

1. Dans Superset, allez dans **Settings** -> **Database Connections**.
2. Cliquez sur **+ Database** -> **PostgreSQL**.
3. Utilisez cette URI de connexion :
   ```text
   postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
   ```
4. Cliquez sur **Connect** puis **Finish**.

Ensuite, allez dans **Datasets** -> **+ Dataset**, choisissez le schéma `analytics`, et vous y trouverez vos 3 tables :

| Table | Description |
| --- | --- |
| `gold_books_details` | 1000 livres détaillés (titre, prix GBP/EUR/USD, stock, avis, catégorie) |
| `gold_books_analytics` | Agrégations par catégorie (nombre, prix moyen, stock total) |
| `gold_countries_enriched` | Pays enrichis (Countries API + Population WorldBank + Taux de change) |

---

## 🏗️ 2. Créer le Dashboard

1. Allez dans **Dashboards** -> **+ DASHBOARD**.
2. Cliquez sur **EDIT DASHBOARD** (icône crayon).
3. Dans le panneau de droite (**Layout Elements**), glissez l'élément **Tabs** en haut.
4. Créez 2 onglets : **Catalogue de Livres** et **Géo-Intelligence**.

---

## 📂 Onglet 1 : Catalogue de Livres (Scraping)

### Dataset : `gold_books_details`

| # | Nom du graphique | Type | Configuration |
| --- | --- | --- | --- |
| 1 | **Répartition par Catégorie** | TreeMap | Group by: `category`, Metric: `COUNT(*)` |
| 2 | **Top 10 livres les plus chers** | Bar Chart | X: `titre`, Metric: `MAX(price_gbp)`, Sort Descending, Row Limit: 10 |
| 3 | **Part du chiffre d'affaires par catégorie** | Pie Chart | Dimension: `category`, Metric: `SUM(price_gbp)` |
| 4 | **Stock Total** | Big Number | Metric: `SUM(stock)` |
| 5 | **Nombre Total de Livres** | Big Number | Metric: `COUNT(*)` |

### Dataset : `gold_books_analytics`

| # | Nom du graphique | Type | Configuration |
| --- | --- | --- | --- |
| 6 | **Prix moyen par catégorie (multi-devises)** | Bar Chart groupé | X: `categorie`, Metrics: `prix_moyen_gbp`, `prix_moyen_eur`, `prix_moyen_usd` |
| 7 | **Stock total par catégorie** | Bar Chart horizontal | X: `categorie`, Metric: `stock_total` |

---

## 📂 Onglet 2 : Géo-Intelligence (APIs)

### Dataset : `gold_countries_enriched`

| # | Nom du graphique | Type | Configuration |
| --- | --- | --- | --- |
| 8 | **Répartition des pays par Région** | Pie Chart | Dimension: `region`, Metric: `COUNT(*)` |
| 9 | **Top 20 pays les plus peuplés** | Bar Chart | X: `pays`, Metric: `MAX(population_banque_mondiale)`, Sort Desc, Limit: 20 |
| 10 | **Population vs Superficie** | Scatter Plot | X: `superficie_km2`, Y: `population_api_countries`, Series: `region` |
| 11 | **Taux de change vs GBP** | Bar Chart | X: `devise_principale`, Metric: `AVG(taux_change_vs_gbp)`, Filter: `taux_change_vs_gbp IS NOT NULL` |
| 12 | **Nombre de pays** | Big Number | Metric: `COUNT(*)` |
| 13 | **Table détaillée des pays** | Table | Colonnes: `pays`, `region`, `capitale`, `population_banque_mondiale`, `devise_principale`, `taux_change_vs_gbp` |

---

## 🧭 3. Requêtes SQL Lab

Si vous préférez coder vos graphiques en SQL, voici les requêtes à copier dans **SQL Lab** :

```sql
-- TreeMap : Livres par catégorie
SELECT category, count(*) AS nb FROM analytics.gold_books_details GROUP BY 1 ORDER BY 2 DESC;

-- Top 10 livres les plus chers
SELECT titre, price_gbp FROM analytics.gold_books_details ORDER BY 2 DESC LIMIT 10;

-- Stock total
SELECT sum(stock) AS stock_total FROM analytics.gold_books_details;

-- Prix moyen par catégorie (multi-devises)
SELECT categorie, prix_moyen_gbp, prix_moyen_eur, prix_moyen_usd
FROM analytics.gold_books_analytics ORDER BY prix_moyen_gbp DESC;

-- Top 20 pays les plus peuplés (données WorldBank)
SELECT pays, population_banque_mondiale, region
FROM analytics.gold_countries_enriched
WHERE population_banque_mondiale IS NOT NULL
ORDER BY population_banque_mondiale DESC LIMIT 20;

-- Taux de change par devise
SELECT devise_principale, AVG(taux_change_vs_gbp) AS taux
FROM analytics.gold_countries_enriched
WHERE taux_change_vs_gbp IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC;

-- Répartition par région
SELECT region, COUNT(*) AS nb_pays
FROM analytics.gold_countries_enriched GROUP BY 1 ORDER BY 2 DESC;
```

---

## 📈 4. Grafana (Observabilité Système)

**URL** : `http://localhost:3001` (admin/admin)

### Panneau "Santé des Conteneurs" (Source: Prometheus)

1. **CPU par Service** :
   - Query : `sum(rate(container_cpu_usage_seconds_total{name=~".+"}[5m])) by (name)`
   - Visualisation : **Time Series**
2. **Mémoire par Service** :
   - Query : `container_memory_usage_bytes{name=~".+"}`
   - Visualisation : **Bar Gauge**

### Importation Rapide

1. Ouvrez Grafana (`localhost:3001`).
2. Allez dans **Dashboards** -> **New** -> **Import**.
3. Cliquez sur **Upload JSON file** et choisissez `grafana_dashboard_africa_techup.json`.
4. Sélectionnez les DataSources (**Prometheus** et **Loki**).

---

## 🛠️ 5. Maintenance

Si vous utilisez **`docker compose down -v`**, vous perdez toutes vos données. Le projet se répare automatiquement au prochain démarrage.

### Procédure de relance :
1. `docker compose up -d`
2. Attendez 1 minute (init-folders + superset-init)
3. Lancez le DAG sur `localhost:8080` -> **`africa_techup_unified_pipeline`**

> 💡 Utilisez `docker compose down` (sans `-v`) pour garder vos données entre deux sessions.
