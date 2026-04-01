# Projet_BigDATA

## Objectif du projet

Concevoir et mettre en oeuvre un pipeline de donnees complet allant de la collecte a la visualisation, en combinant des donnees issues d'une API et du scraping web.

Le projet couvre :

- Ingestion de donnees depuis une API
- Scraping de donnees
- Stockage des donnees brutes (Data Lake)
- Transformation des donnees avec Python
- Chargement dans PostgreSQL (Data Warehouse)
- Creation d'un tableau de bord interactif
- Preparation d'une presentation de soutenance

## Technologies utilisees

Python

- API (requests)
- Scraping (BeautifulSoup / Selenium)
- Transformation (pandas)

Data Lake

- JSON / CSV

PostgreSQL

- Stockage structure

Dashboard

- Streamlit
- ou Power BI

## Workflow du projet

- Ingestion (API + scraping)
- Stockage brut
- Transformation et fusion
- Chargement en base
- Visualisation

## Stockage MinIO (obligatoire)

Le pipeline n'utilise plus le stockage local. Les donnees passent par MinIO.
Definir les variables d'environnement suivantes avant de lancer le pipeline :

```bash
STORAGE_BACKEND=minio
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
MINIO_BUCKET=velostar
MINIO_SECURE=false
```

`STORAGE_BACKEND` doit etre positionne a `minio`.