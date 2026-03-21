# 🎤 Script de Présentation : Projet Data Engineering - Africa Techup Tour

Ce document est une transcription détaillée pour vous aider à présenter votre projet à l'équipe Africa Techup Tour. Vous pouvez le lire, vous l'approprier et l'utiliser comme fil conducteur lors de votre présentation.

---

## 1. Introduction (1 minute)

**"Bonjour à toutes et à tous,"**

"Je suis ravi de vous présenter aujourd'hui l'aboutissement de mon projet fil conducteur, développé dans le cadre de ma candidature pour le bootcamp Africa Techup Tour. Mon objectif était de concevoir non pas un simple script de traitement de données, mais un **véritable pipeline ETL industrialisé de bout en bout**, digne des architectures modernes rencontrées en entreprise."

"Ce projet démontre mes compétences en **Data Engineering**, en **DevOps**, en **Orchestration** et en **Observabilité**."

---

## 2. Le Besoin et la Vision Globale (1 minute)

"Le défi technique que je me suis lancé était d'ingérer des données issues de sources hétérogènes, de les nettoyer, de les croiser, puis de les rendre disponibles pour l'analyse et la Business Intelligence, le tout de manière **100% automatisée**, **scalable** et **monitorée**."

"Pour y parvenir, j'ai implémenté une **Architecture Medallion (Bronze, Silver, Gold)**. Au lieu de stocker les données n'importe où, elles traversent plusieurs zones de raffinement."

---

## 3. L'Architecture et les Choix Techniques (3 minutes)

*Ici, vous pouvez montrer le schéma de votre README.*

"Mon architecture repose sur plusieurs briques open-source modernes, entièrement conteneurisées via **Docker Compose** (je fais tourner jusqu'à 15 services simultanément) :"

### A. L'Extraction (La couche Bronze)
"J'ai utilisé Python pour développer des connecteurs robustes pointant vers 4 sources de données différentes : 
1. Du Web Scraping ciblé sur le site *Books to Scrape* (1000 livres).
2. L'API REST *RestCountries* pour les données démographiques.
3. L'API historique de la *World Bank* gérant la pagination complexe.
4. L'API *ExchangeRate* pour récupérer les taux de change de la livre sterling (GBP) vers de multiples devises (USD, EUR, etc.)."

"Cette donnée brute est poussée telle quelle dans mon Data Lake **MinIO (compatible S3)**, formant ma couche **Bronze**. MinIO me permet de garantir la pérennité et le découplage du stockage."

### B. La Transformation (La couche Silver)
"Pour le traitement des données (nettoyage des textes, conversion des devises, typage strict), j'ai volontairement utilisé deux moteurs différents pour prouver ma polyvalence :
- **Pandas** : Parfait pour le traitement rapide et en mémoire des jeux de données moyens.
- **Apache Spark** : Déployé en architecture Master/Worker, idéal pour les gros volumes de données nécessitant une exécution distribuée. J'utilise même le format de stockage *Parquet* pour optimiser la lecture."
"Les données propres sont ensuite écrites dans la couche **Silver** de MinIO."

### C. Le Chargement et le Stockage (La couche Gold)
"Pour répondre aux besoins métiers et réaliser des agrégations avancées, j'ai implémenté la dernière étape : la couche **Gold**. Les données valorisées sont poussées par la méthode UPSERT vers un Data Warehouse relationnel, ici **PostgreSQL**, spécifiquement dans un schéma nommé `analytics`."

### D. L'Orchestration au cœur du réacteur
"La véritable force de mon projet réside dans son orchestration. L'ensemble de ce flux n'est pas lancé manuellement. J'ai utilisé **Apache Airflow**. J'ai développé des DAGs (Directed Acyclic Graphs) très précis qui assurent que les extractions s'exécutent en parallèle, que les transformations attendent les bonnes dépendances, et que les échecs soient gérés sans bloquer le reste de la pipeline."

---

## 4. Visualisation, BI et Observabilité (DataOps) (2 minutes)

"Savoir traiter la donnée, c'est bien. Savoir la rendre exploitable et maintenir la plateforme, c'est le métier d'un vrai Data Engineer."

"Pour clôturer le cycle de vie de la donnée, j'ai déployé **Apache Superset** et **Grafana**. Superset permet aux équipes métier de se connecter au schéma Gold de PostgreSQL pour créer des rapports d'Intelligence Artificielle (BI) dynamiques."

"Mais je suis allé plus loin en intégrant une approche très **DevOps/DataOps** : j'ai mis en place un monitoring complet de mon infrastructure."
- "**Prometheus** et **cAdvisor** surveillent en temps réel l'état de santé (CPU/RAM) de tous mes conteneurs Docker (Spark, Airflow, etc.)."
- "**Loki** et **Promtail** interceptent, centralisent et agrègent tous les logs de mes pipelines."
"Tout cet écosystème d'observabilité est directement visualisable via des dashboards dans **Grafana**. Si un job Spark échoue ou si Airflow utilise trop de mémoire, je le vois immédiatement."

---

## 5. Qualité, CI/CD et Bonnes pratiques (1 minute)

"Le code a été écrit en suivant les standards de l'industrie. J'ai créé **6 fichiers de tests automatisés** avec `Pytest`. De plus, j'ai implémenté une pipeline **CI/CD via GitHub Actions** qui s'assure à chaque commit que le code respecte les normes de formatage `flake8` et vérifie si mes images Docker buildent correctement."

---

## 6. Conclusion (30 secondes)

"Pour conclure, ce projet m'a permis de toucher à toutes les problématiques concrètes d'une infrastructure Data d'entreprise : la robustesse de l'extraction, la flexibilité avec Pandas/Spark, le stockage de type Lakehouse avec MinIO/PostgreSQL, l'orchestration avancée avec Airflow, la Data Visualization avec Superset, et enfin le monitoring de pointe avec Prometheus/Grafana/Loki."

"Je suis convaincu de disposer d'un socle technique exceptionnellement solide pour intégrer le bootcamp Africa Techup Tour et je suis prêt à le mettre au service de cas d'usage encore plus complexes. Je vous remercie et suis disponible pour toutes vos questions ou pour vous montrer une démonstration live."
