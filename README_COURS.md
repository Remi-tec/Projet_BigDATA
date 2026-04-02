# Cours et documentation du pipeline Velostar

## 1) Objectif pedagogique

Construire un pipeline de donnees complet, depuis la collecte (API GBFS) jusqu au chargement en base PostgreSQL, en passant par un Data Lake MinIO et une phase de transformation avec pandas. Le but est de comprendre le cheminement des donnees, les choix techniques, et le role de chaque fonction.

Fichiers utiles
- [pipeline_velostar.py](pipeline_velostar.py)
- [test_pipeline_velostar.py](test_pipeline_velostar.py)
- [docker-compose.yml](docker-compose.yml)

## 2) Technologies en detail (quoi, pourquoi, comment)

### 2.1 API GBFS (General Bikeshare Feed Specification)

Quoi
- Format standard pour publier des donnees de velos en libre service.
- Fournit un index qui reference les flux disponibles (station_information, station_status, etc.).

Pourquoi
- Standardise la structure des donnees.
- Permet de parser facilement plusieurs flux.

Comment dans le projet
- Appel d un index pour obtenir les URL des flux.
- Telechargement de deux flux cibles: station_information et station_status.

### 2.2 requests (HTTP)

Quoi
- Bibliotheque Python pour faire des requetes HTTP.

Pourquoi
- Simple pour consommer une API REST.
- Gestion des timeouts et des erreurs via raise_for_status.

Comment dans le projet
- requests.get(url, timeout=15) pour recuperer l index et les flux.
- response.json() pour decoder les reponses JSON.

### 2.3 pandas (transformation)

Quoi
- Bibliotheque d analyse de donnees en tableaux (DataFrame).

Pourquoi
- Permet normalisation, typage, jointure et calculs en peu de lignes.
- Ideale pour preparer un CSV propre avant chargement en base.

Comment dans le projet
- Creation de DataFrame depuis les listes de stations.
- Conversion des types (int, float, datetime).
- Jointure sur station_id.
- Calculs: fill_rate, availability.

### 2.4 MinIO (Data Lake)

Quoi
- Stockage objet compatible S3, auto heberge.

Pourquoi
- Centralise les fichiers bruts et transformes.
- Permet decouplage ingestion vs transformation.
- Facile a exposer en environnement local.

Comment dans le projet
- Sauvegarde JSON bruts dans un dossier de batch horodate.
- Sauvegarde CSV transformes dans un dossier processed.
- Lecture des objets par cle (chemin S3).

### 2.5 PostgreSQL (Data Warehouse)

Quoi
- Base relationnelle robuste pour l analyse.

Pourquoi
- Stocke les informations statiques (stations_info) et l historique des statuts (stations_status).
- Permet des requetes analytiques et un futur dashboard.

Comment dans le projet
- Creation des tables si absentes.
- UPSERT sur stations_info.
- INSERT append sur stations_status pour historiser.

### 2.6 SQLAlchemy (connexion SQL)

Quoi
- Couche Python pour se connecter et executer du SQL.

Pourquoi
- Simplifie la creation de moteur et l execution de requetes SQL.

Comment dans le projet
- create_engine pour la connexion.
- conn.execute(text(...)) pour le DDL et l UPSERT.

### 2.7 python-dotenv (configuration)

Quoi
- Charge un fichier .env dans les variables d environnement.

Pourquoi
- Evite de coder en dur les secrets et URLs.

Comment dans le projet
- load_dotenv() au demarrage du module.
- os.getenv(...) pour recuperer les valeurs.

### 2.8 pytest + mocks (tests)

Quoi
- pytest: framework de tests.
- mocks: simulation d API et de stockage.

Pourquoi
- Tester la logique sans reseau ni base reelle.
- Isoler chaque classe.

Comment dans le projet
- DummyStorage en memoire.
- patch sur requests.get.
- Verification des DataFrame et des appels SQL.

### 2.9 Docker Compose (infra locale)

Quoi
- Definition d un service MinIO local.

Pourquoi
- Lancer rapidement un Data Lake sans installation complexe.

Comment dans le projet
- docker compose up -d pour demarrer MinIO.

## 3) Architecture du pipeline

Flux principal
1. Ingestion: recuperation de l index GBFS, puis des flux cibles.
2. Stockage brut: sauvegarde JSON dans MinIO.
3. Transformation: normalisation et enrichissement dans pandas.
4. Stockage traite: export CSV dans MinIO.
5. Chargement: insertion dans PostgreSQL.

Classes principales
- GBFSIngester: collecte et stockage brut
- VeloStarTransformer: transformation et export CSV
- PostgreSQLLoader: chargement en base
- VeloStarPipeline: orchestration globale

### 3.1 Schema d architecture (Mermaid)

```mermaid
flowchart LR
    A[API GBFS Rennes] --> B[GBFSIngester]
    B --> C[(MinIO - Data Lake)
JSON bruts]
    C --> D[VeloStarTransformer]
    D --> E[(MinIO - CSV)
donnees traitees]
    E --> F[PostgreSQLLoader]
    F --> G[(PostgreSQL)
Data Warehouse]
```

## 4) Prerequis et configuration

### 4.1 MinIO (obligatoire)

Lancer MinIO via Docker:

```bash
docker compose up -d
```

Variables d environnement requises:

```bash
STORAGE_BACKEND=minio
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
MINIO_BUCKET=velostar
MINIO_SECURE=false
```

### 4.2 PostgreSQL (non fournie ici)

Il faut un PostgreSQL accessible via les variables habituelles (host, user, password, db). Les details sont a renseigner dans l environnement avant execution du pipeline.

## 5) Execution pas a pas

### 5.1 Lancer le pipeline complet

```bash
python pipeline_velostar.py
```

Ce script enchaine ingestion, transformation, chargement.

### 5.2 Lancer uniquement les tests

```bash
pytest -v
```

## 6) Structure du code et details par fonction

Toutes les fonctions sont dans [pipeline_velostar.py](pipeline_velostar.py). Ci dessous: role, entrees, sorties et usage.

### 6.1 Stockage (StorageBackend, MinIOStorage)

StorageBackend (interface)
- __init__ : aucune logique, sert de contrat.
- make_batch_folder(base_dir): entree base_dir (str) -> sortie chemin batch (str). Usage: creer un dossier horodate.
- save_json(folder, filename, data): entree dict -> sortie cle objet (str). Usage: sauver un JSON brut.
- load_json(folder, filename): entree (folder, filename) -> sortie dict. Usage: relire un JSON brut.
- save_csv(df, processed_dir, filename): entree DataFrame -> sortie cle objet (str). Usage: sauver un CSV traite.
- load_csv(path, **kwargs): entree cle -> sortie DataFrame. Usage: relire un CSV.
- list_batches(base_dir): entree base_dir -> sortie liste de batches. Usage: recuperer le dernier batch.
- list_csvs(processed_dir): entree processed_dir -> sortie liste CSV. Usage: recuperer le dernier CSV.

MinIOStorage (implementation)
- __init__(endpoint, access_key, secret_key, bucket, secure): entree config MinIO -> sortie instance prete. Usage: connexion client MinIO et creation bucket si absent.
- _clean_path(path): entree str -> sortie str. Usage: normaliser chemin pour MinIO.
- make_batch_folder(base_dir): entree base_dir -> sortie chemin horodate. Usage: generer cle de batch.
- save_json(folder, filename, data): entree dict -> sortie cle s3. Usage: encode JSON et upload.
- load_json(folder, filename): entree cle -> sortie dict. Usage: download et decode.
- save_csv(df, processed_dir, filename): entree DataFrame -> sortie cle s3. Usage: export CSV et upload.
- load_csv(path, **kwargs): entree cle -> sortie DataFrame. Usage: download CSV et lecture pandas.
- list_batches(base_dir): entree prefix -> sortie liste batches tries. Usage: trouver dernier batch.
- list_csvs(processed_dir): entree prefix -> sortie liste CSV. Usage: trouver dernier CSV velostar_*.csv.

create_storage_from_env
- Entree: variables d environnement (STORAGE_BACKEND, MINIO_*).
- Sortie: instance de StorageBackend (MinIOStorage).
- Usage: centraliser la creation du stockage et forcer MinIO.

### 6.2 Ingestion (GBFSIngester)

__init__(data_lake_dir, storage)
- Entree: dossier raw et stockage optionnel.
- Sortie: instance prete a ingerer.
- Usage: configure l origine et le backend.

run()
- Entree: aucune.
- Sortie: chemin du batch cree (str).
- Usage: pipeline complet d ingestion: index, flux, sauvegardes.

fetch_index()
- Entree: aucune.
- Sortie: dict {nom_flux: url}.
- Usage: methode exposee pour les tests.

fetch_feed(url, name)
- Entree: url flux, nom flux.
- Sortie: dict (JSON GBFS).
- Usage: methode exposee pour les tests.

_fetch_index()
- Entree: aucune.
- Sortie: dict {idfilegbfs: filegbfsurl}.
- Usage: appeler l API pour obtenir la liste des flux.

_fetch_feed(url, name)
- Entree: url flux, nom flux.
- Sortie: dict (JSON GBFS).
- Usage: telecharger un flux cible.

_add_metadata(data, source)
- Entree: data (dict), source (str).
- Sortie: dict enrichi avec _metadata.
- Usage: tracer la source et le timestamp de collecte.

_save(data, filename)
- Entree: dict, nom de fichier logique.
- Sortie: cle de stockage (str).
- Usage: deleguer la sauvegarde au backend.

### 6.3 Transformation (VeloStarTransformer)

__init__(data_lake_dir, processed_dir, storage)
- Entree: chemins et stockage.
- Sortie: instance prete a transformer.
- Usage: configure l origine et la destination.

run(batch_folder=None)
- Entree: batch_folder optionnel (str).
- Sortie: (DataFrame final, cle CSV).
- Usage: transformer un batch (ou le dernier) et exporter CSV.

build_information_df(stations)
- Entree: liste de stations (dict).
- Sortie: DataFrame normalise.
- Usage: typer et selectionner les colonnes info.

build_status_df(stations)
- Entree: liste de stations (dict).
- Sortie: DataFrame normalise.
- Usage: typer et convertir last_reported en datetime.

merge_and_enrich(df_info, df_status, collected_at)
- Entree: DataFrame info + status, collected_at (str).
- Sortie: DataFrame final avec indicateurs.
- Usage: jointure et calcul des colonnes metier.

_get_latest_batch()
- Entree: aucune.
- Sortie: chemin du batch le plus recent.
- Usage: choix automatique du dernier batch.

_load_json(folder, filename)
- Entree: dossier batch, nom de fichier.
- Sortie: dict JSON.
- Usage: lecture d un JSON brut.

_extract_stations(wrapper)
- Entree: wrapper GBFS avec data/data/stations.
- Sortie: liste de stations.
- Usage: extraire la liste brute.

_compute_availability(row)
- Entree: ligne pandas.
- Sortie: str (Disponible, Indisponible, Hors service).
- Usage: derivation d un label lisible.

_export_csv(df)
- Entree: DataFrame final.
- Sortie: cle CSV sauvegardee.
- Usage: horodatage + export MinIO.

_log_stats(df)
- Entree: DataFrame final.
- Sortie: aucune.
- Usage: logging des stats principales.

### 6.4 Chargement (PostgreSQLLoader)

__init__(host, port, dbname, user, password, processed_dir, storage)
- Entree: credentials et stockage.
- Sortie: instance avec moteur SQL.
- Usage: prepare le chargement.

run(csv_path=None)
- Entree: cle CSV optionnelle.
- Sortie: aucune.
- Usage: cree tables, charge CSV, insere en base.

create_tables()
- Entree: aucune.
- Sortie: aucune.
- Usage: execute le DDL si tables absentes.

load_stations_info(df)
- Entree: DataFrame complet.
- Sortie: aucune.
- Usage: UPSERT des informations statiques.

load_stations_status(df)
- Entree: DataFrame complet.
- Sortie: aucune.
- Usage: INSERT append des statuts.

count(table)
- Entree: nom de table.
- Sortie: int.
- Usage: utilitaire pour tests ou verification.

_create_engine(host, port, dbname, user, password)
- Entree: credentials.
- Sortie: moteur SQLAlchemy.
- Usage: centraliser la creation du moteur.

_get_latest_csv()
- Entree: aucune.
- Sortie: cle du CSV le plus recent.
- Usage: choix automatique du dernier CSV.

_log_counts()
- Entree: aucune.
- Sortie: aucune.
- Usage: logging des compteurs post chargement.

### 6.5 Orchestration (VeloStarPipeline)

__init__(data_lake_dir, processed_dir)
- Entree: chemins de travail.
- Sortie: instance avec les trois composants.
- Usage: initialiser l ingestion, la transformation, le chargement.

run()
- Entree: aucune.
- Sortie: aucune.
- Usage: enchaine les trois etapes dans l ordre.

### 6.6 Point d entree

if __name__ == "__main__": VeloStarPipeline().run()
- Entree: execution du script.
- Sortie: pipeline complet.
- Usage: lancement direct depuis la ligne de commande.

## 7) Conseils de verification

- Verifier que MinIO est accessible via http://localhost:9001
- Verifier la creation du bucket et des objets JSON/CSV
- Lancer les tests unitaires pour valider la logique de transformation

## 8) Exercices proposes

1) Ajouter un nouveau flux GBFS si disponible dans l index.
2) Calculer un indicateur de disponibilite en pourcentage de docks libres.
3) Ajouter un filtre pour ne conserver que les stations actives.
4) Creer un dashboard simple (Streamlit) a partir du CSV exporte.
