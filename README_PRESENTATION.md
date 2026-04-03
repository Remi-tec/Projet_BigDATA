
# Présentation détaillée du projet VéloStar Rennes

## 1. Contexte et Objectif
Le projet VéloStar Rennes a pour ambition de mettre en place une chaîne de traitement de données complète, allant de la collecte à la valorisation, autour du service de vélos en libre-service de la métropole rennaise. L'objectif principal est de permettre la collecte automatisée de données issues de l'API GBFS (General Bikeshare Feed Specification), leur transformation, leur stockage sécurisé et leur exploitation à travers des outils d'analyse et de visualisation. Ce projet s'adresse aussi bien aux équipes opérationnelles qu'aux citoyens ou aux analystes de données, en fournissant des indicateurs temps réel et historiques pour le suivi et l'optimisation du service.

## 2. Architecture Générale
L'architecture du pipeline de données est pensée pour être modulaire, automatisée et robuste. Elle se compose de plusieurs étapes successives :
- **Ingestion** : Cette étape consiste à interroger automatiquement l'API GBFS afin de récupérer les données brutes concernant les stations et leur statut en temps réel. Les fichiers ciblés sont notamment `station_information` et `station_status`.
- **Stockage brut (Data Lake)** : Les données collectées sont stockées sous forme de fichiers JSON dans un espace de stockage objet MinIO, compatible avec le protocole S3. Ce choix permet de garantir la pérennité, la sécurité et la scalabilité du stockage des données brutes.
- **Transformation** : Les données brutes sont ensuite nettoyées, normalisées et enrichies à l'aide de la bibliothèque pandas. Cette étape inclut la fusion des différentes sources, la conversion des formats, l'ajout de nouveaux indicateurs et la préparation des données pour l'analyse.
- **Stockage structuré (Data Warehouse)** : Les données transformées sont chargées dans une base de données PostgreSQL, ce qui permet d'effectuer des requêtes analytiques complexes et d'assurer la traçabilité des historiques.
- **Visualisation** : Un tableau de bord interactif, développé avec Streamlit, permet d'explorer les données en temps réel et de visualiser l'évolution du service sur différentes périodes.
- **Orchestration** : L'ensemble du pipeline est automatisé grâce à Apache Airflow, qui planifie et exécute les différentes tâches à intervalles réguliers (par exemple, toutes les minutes).

## 3. Technologies exploitées
Le projet s'appuie sur un ensemble de technologies modernes et éprouvées :
- **Python** est le langage principal utilisé pour le développement de tous les modules du pipeline, de l'ingestion à la visualisation.
- **MinIO** est utilisé comme solution de stockage objet pour le Data Lake. Il remplace le stockage local traditionnel et permet une compatibilité avec les solutions cloud S3.
- **PostgreSQL** sert de base de données relationnelle pour l'entreposage structuré des données et la réalisation d'analyses avancées.
- **Pandas** est la bibliothèque de référence pour la manipulation, le nettoyage et la fusion des données tabulaires.
- **SQLAlchemy** facilite les interactions entre Python et PostgreSQL grâce à une interface ORM puissante et flexible.
- **Streamlit** permet de créer rapidement des applications web interactives pour la visualisation et l'exploration des données par les utilisateurs finaux.
- **Plotly** est utilisé pour produire des visualisations graphiques avancées, telles que des cartes interactives et des courbes temporelles.
- **Apache Airflow** orchestre et planifie l'exécution des différentes étapes du pipeline via des DAGs (Directed Acyclic Graphs).
- **Docker** assure la conteneurisation et le déploiement cohérent de tous les services nécessaires (Airflow, PostgreSQL, MinIO, etc.), facilitant ainsi la portabilité et la reproductibilité de l'environnement.

## 4. Structure des données échangées

### Exemples concrets de structures de données

#### a) Exemple de données brutes reçues depuis l'API GBFS (JSON)

**station_information.json**
```json
{
  "data": {
    "stations": [
      {
        "station_id": "1001",
        "name": "République",
        "address": "Place de la République",
        "post_code": "35000",
        "lat": 48.11198,
        "lon": -1.68037,
        "capacity": 20
      },
      ...
    ]
  }
}
```

**station_status.json**
```json
{
  "data": {
    "stations": [
      {
        "station_id": "1001",
        "num_bikes_available": 7,
        "num_docks_available": 13,
        "is_installed": true,
        "is_renting": true,
        "is_returning": true,
        "last_reported": 1712131200
      },
      ...
    ]
  }
}
```

Chaque fichier est enrichi lors de la collecte d'un bloc `_metadata` :
```json
{
  "_metadata": {
    "source": "https://api-gbfs.rennesmetropole.fr/gbfs/2.2/gbfs.json",
    "collected_at": "2026-04-03T10:00:00+00:00",
    "dataset": "vls-gbfs-tr"
  },
  "data": { ... }
}
```

#### b) Exemple de données transformées stockées dans MinIO (CSV)

| station_id | name         | address                  | post_code | lat      | lon      | capacity | num_bikes_available | num_docks_available | fill_rate | availability  | is_installed | is_renting | is_returning | last_reported        | collected_at          |
|------------|--------------|--------------------------|-----------|----------|----------|----------|---------------------|--------------------|-----------|---------------|--------------|------------|--------------|----------------------|----------------------|
| 1001       | République   | Place de la République   | 35000     | 48.11198 | -1.68037 | 20       | 7                   | 13                 | 35.0      | Disponible    | True         | True       | True         | 2026-04-03 10:00:00 | 2026-04-03 10:00:00  |
| ...        | ...          | ...                      | ...       | ...      | ...      | ...      | ...                 | ...                | ...       | ...           | ...          | ...        | ...          | ...                 | ...                  |

#### c) Structure des tables PostgreSQL

**stations_info**
| Champ      | Type              | Description                                 |
|------------|-------------------|---------------------------------------------|
| station_id | VARCHAR(10)       | Identifiant unique de la station            |
| name       | VARCHAR(100)      | Nom de la station                           |
| address    | VARCHAR(200)      | Adresse                                     |
| post_code  | VARCHAR(10)       | Code postal                                 |
| lat        | DOUBLE PRECISION  | Latitude                                    |
| lon        | DOUBLE PRECISION  | Longitude                                   |
| capacity   | INTEGER           | Capacité totale                             |
| updated_at | TIMESTAMP WITH TZ | Date de dernière mise à jour                |

**stations_status**
| Champ                | Type              | Description                                 |
|----------------------|-------------------|---------------------------------------------|
| id                   | SERIAL PRIMARY KEY| Identifiant unique du snapshot              |
| station_id           | VARCHAR(10)       | Identifiant de la station (clé étrangère)   |
| num_bikes_available  | INTEGER           | Nombre de vélos disponibles                 |
| num_docks_available  | INTEGER           | Nombre de places libres                     |
| fill_rate            | NUMERIC(5,1)      | Taux de remplissage (%)                     |
| availability         | VARCHAR(20)       | Label de disponibilité calculé              |
| is_installed         | BOOLEAN           | Station installée ou non                    |
| is_renting           | BOOLEAN           | Location possible                           |
| is_returning         | BOOLEAN           | Retour possible                             |
| last_reported        | TIMESTAMP WITH TZ | Dernier rapport de statut                   |
| collected_at         | TIMESTAMP WITH TZ | Date de collecte du snapshot                |

Ces exemples permettent de visualiser concrètement la structure des données à chaque étape du pipeline, depuis la collecte brute jusqu'à l'entreposage structuré.
### a) Données brutes (Data Lake MinIO)
Les données brutes sont stockées dans MinIO sous forme de fichiers JSON. Deux fichiers principaux sont collectés à chaque exécution du pipeline :
- **station_information.json** : Ce fichier contient la description statique de chaque station, incluant l'identifiant unique de la station (`station_id`), son nom, son adresse, son code postal, sa latitude, sa longitude et sa capacité totale d'accueil de vélos.
- **station_status.json** : Ce fichier fournit le statut temps réel de chaque station, avec des informations telles que le nombre de vélos disponibles, le nombre de places libres, l'état d'installation, la possibilité de louer ou de rendre un vélo, ainsi que le dernier horodatage de mise à jour.
Chaque fichier JSON est enrichi d'un bloc de métadonnées (`_metadata`) qui précise la source, la date et l'heure de collecte, ainsi que le nom du dataset, afin d'assurer la traçabilité et la qualité des données.

### b) Données transformées (CSV MinIO)
Après transformation, les données issues des deux flux sont fusionnées sur la clé `station_id`. De nouveaux champs sont ajoutés, tels que le taux de remplissage (`fill_rate`, exprimé en pourcentage), un label de disponibilité calculé (`availability`), et l'horodatage de collecte (`collected_at`). Ces données sont exportées au format CSV dans MinIO, prêtes à être chargées dans la base de données ou utilisées pour des analyses ultérieures.

### c) Données structurées (PostgreSQL)
Dans la base PostgreSQL, deux tables principales sont utilisées :
- **stations_info** : Cette table contient les informations statiques sur les stations. Elle est alimentée par un mécanisme d'UPSERT, ce qui signifie que chaque station est insérée ou mise à jour en fonction de son identifiant unique. Les champs incluent l'identifiant, le nom, l'adresse, le code postal, la latitude, la longitude, la capacité et la date de dernière mise à jour.
- **stations_status** : Cette table enregistre l'historique des statuts temps réel des stations. Chaque nouvelle collecte donne lieu à un INSERT, permettant de conserver un historique complet des disponibilités et des usages. Les champs incluent un identifiant unique, l'identifiant de la station, le nombre de vélos et de places disponibles, le taux de remplissage, le label de disponibilité, les états d'installation et de location, les horodatages de rapport et de collecte.

## 5. Pipeline technique détaillé
Le pipeline technique est composé de plusieurs modules spécialisés :
- **GBFSIngester** : Ce module se charge de récupérer l'index des flux disponibles via l'API GBFS, puis de télécharger les fichiers ciblés (station_information et station_status). Chaque flux est sauvegardé dans MinIO avec ses métadonnées associées, garantissant ainsi la traçabilité de chaque collecte.
- **VeloStarTransformer** : Ce module lit les fichiers JSON bruts depuis MinIO, normalise et fusionne les données sur la clé `station_id`, calcule de nouveaux indicateurs (taux de remplissage, label de disponibilité), convertit les timestamps et exporte le résultat final au format CSV dans MinIO.
- **PostgreSQLLoader** : Ce module gère la création des tables dans PostgreSQL si elles n'existent pas encore, charge le fichier CSV transformé, effectue un UPSERT des données statiques dans la table stations_info et insère les snapshots temps réel dans la table stations_status.
- **VeloStarPipeline** : Il s'agit de l'orchestrateur principal qui exécute les trois étapes précédentes dans l'ordre, assurant ainsi la cohérence et la fluidité du traitement des données.
- **Airflow** : Un DAG Airflow planifie et exécute automatiquement le pipeline à intervalles réguliers (par exemple, chaque minute), garantissant ainsi une actualisation continue des données.

## 6. Visualisation et usages
La valorisation des données est assurée par un tableau de bord interactif développé avec Streamlit :
- **Dashboard Streamlit** : L'utilisateur accède à une carte interactive affichant l'état en temps réel de toutes les stations, avec des indicateurs visuels sur le taux de remplissage et la capacité. Il peut sélectionner une station pour obtenir une analyse détaillée, visualiser l'historique de disponibilité sur une journée et une plage horaire choisie, et explorer les données à des fins opérationnelles, analytiques ou de communication.
- **Autres usages** : Les données structurées peuvent être extraites pour des analyses avancées, telles que la prévision de la demande, le clustering de stations, ou encore le suivi de la performance du service. Elles servent également de support à la prise de décision pour la gestion et l'optimisation du réseau de vélos en libre-service.

## 7. Sécurité, configuration et bonnes pratiques
Le projet intègre plusieurs mécanismes de sécurité et de bonnes pratiques :
- **Variables d'environnement** : La configuration du stockage, de la base de données et des accès API est entièrement gérée par des variables d'environnement, ce qui permet de sécuriser les informations sensibles et de faciliter le déploiement sur différents environnements.
  - Pour le stockage : `STORAGE_BACKEND=minio`, `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`, `MINIO_SECURE`.
  - Pour la base de données : `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.
  - Pour l'API : `GBFS_INDEX_URL`.
- **Logs** : Un système de journalisation détaillé est mis en place à chaque étape du pipeline, permettant de suivre l'exécution, de détecter les erreurs et d'assurer la traçabilité des traitements.
- **Tests unitaires** : Un module de tests permet de valider le bon fonctionnement de chaque composant du pipeline, garantissant ainsi la fiabilité et la maintenabilité du projet.
- **Conteneurisation** : Tous les services nécessaires au projet sont conteneurisés via Docker Compose, ce qui simplifie le déploiement, la montée en charge et la reproductibilité de l'environnement de travail.

## 8. Points forts et perspectives
Le projet VéloStar Rennes présente plusieurs atouts majeurs :
- Son architecture modulaire et testable facilite l'évolution et l'adaptation à d'autres contextes ou jeux de données.
- L'automatisation complète du pipeline grâce à Airflow garantit une actualisation continue et fiable des données.
- Le choix d'un stockage objet cloud-ready (MinIO) permet d'envisager facilement une migration vers des infrastructures cloud publiques ou hybrides.
- La visualisation avancée et interactive des données favorise l'appropriation des résultats par différents types d'utilisateurs.
- Enfin, la solution est conçue pour être facilement transposable à d'autres villes ou services de mobilité partagée, ouvrant la voie à de nombreuses perspectives d'extension et d'innovation.

---

*Ce document présente l'intégralité du pipeline, la structure des données, les technologies et les usages pour une compréhension approfondie et contextualisée du projet VéloStar Rennes.*
