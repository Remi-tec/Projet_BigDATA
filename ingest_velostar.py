"""
=============================================================
  Ingestion API - VéloStar Rennes (GBFS / OpenDataSoft)
=============================================================
Ce script collecte les données en temps réel des stations
VéloStar de Rennes et les sauvegarde dans le Data Lake local
(dossier raw/).

Données récoltées :
  - station_information : nom, localisation (lat/lon), capacité
  - station_status      : vélos dispo, places dispo, état

Standard : GBFS (General Bikeshare Feed Specification)
Source    : https://data.rennesmetropole.fr/explore/dataset/vls-gbfs-tr
"""

import os
import json
import logging
import requests
from datetime import datetime

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

# Endpoints GBFS du STAR Rennes (Opendatasoft)
BASE_URL = "https://data.rennesmetropole.fr/api/explore/v2.1/catalog/datasets"
DATASET  = "vls-gbfs-tr"

# Fichiers GBFS disponibles dans le dataset
GBFS_FILES = {
    "station_information": "station_information",
    "station_status":      "station_status",
}

# Dossier de stockage brut (Data Lake local)
DATA_LAKE_DIR = os.path.join("raw", "velostar")

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Fonctions utilitaires
# ─────────────────────────────────────────────

def create_data_lake_dir() -> str:
    """Crée le dossier de stockage horodaté pour cette collecte."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder = os.path.join(DATA_LAKE_DIR, timestamp)
    os.makedirs(folder, exist_ok=True)
    logger.info(f"Dossier Data Lake créé : {folder}")
    return folder


def fetch_gbfs_file(feed_name: str) -> dict:
    """
    Récupère un fichier GBFS depuis l'API Opendatasoft.

    L'API expose les enregistrements GBFS sous forme de records,
    chaque record contenant le contenu JSON du flux correspondant.
    """
    url = f"{BASE_URL}/{DATASET}/records"
    params = {
        "where": f"name='{feed_name}'",
        "limit": 1,
    }

    logger.info(f"Requête API → {feed_name}")
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()

    data = response.json()
    logger.info(f"Réponse reçue pour '{feed_name}' (status {response.status_code})")
    return data


def fetch_all_records(limit: int = 100) -> dict:
    """
    Récupère tous les enregistrements du dataset d'un coup.
    Utile pour explorer la structure complète.
    """
    url = f"{BASE_URL}/{DATASET}/records"
    params = {"limit": limit}

    logger.info("Récupération de tous les enregistrements du dataset...")
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()

    data = response.json()
    total = data.get("total_count", "?")
    logger.info(f"Total enregistrements disponibles : {total}")
    return data


def save_to_lake(data: dict, folder: str, filename: str) -> str:
    """Sauvegarde un dictionnaire JSON dans le Data Lake."""
    filepath = os.path.join(folder, f"{filename}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"Sauvegardé : {filepath}")
    return filepath


def add_metadata(data: dict, source: str) -> dict:
    """Enrichit les données brutes avec des métadonnées de collecte."""
    return {
        "_metadata": {
            "source":       source,
            "collected_at": datetime.now().isoformat(),
            "dataset":      DATASET,
        },
        "data": data,
    }


# ─────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────

def run_ingestion():
    """Lance le pipeline complet d'ingestion."""
    logger.info("=" * 50)
    logger.info("  Démarrage de l'ingestion VéloStar Rennes")
    logger.info("=" * 50)

    # 1. Créer le dossier de stockage horodaté
    folder = create_data_lake_dir()

    collected_files = []

    # 2. Récupérer tous les enregistrements du dataset (vue globale)
    try:
        all_records = fetch_all_records(limit=100)
        enriched = add_metadata(all_records, source=f"{BASE_URL}/{DATASET}/records")
        path = save_to_lake(enriched, folder, "all_records")
        collected_files.append(path)
    except requests.RequestException as e:
        logger.error(f"Erreur lors de la récupération globale : {e}")

    # 3. Tentative de récupération par fichier GBFS spécifique
    for feed_name, filename in GBFS_FILES.items():
        try:
            raw_data = fetch_gbfs_file(feed_name)
            enriched = add_metadata(raw_data, source=f"{BASE_URL}/{DATASET}/records?where=name='{feed_name}'")
            path = save_to_lake(enriched, folder, filename)
            collected_files.append(path)
        except requests.RequestException as e:
            logger.warning(f"Impossible de récupérer '{feed_name}' : {e}")

    # 4. Résumé de la collecte
    logger.info("-" * 50)
    logger.info(f"Ingestion terminée. {len(collected_files)} fichier(s) sauvegardé(s) :")
    for f in collected_files:
        logger.info(f"  → {f}")
    logger.info("=" * 50)

    return folder


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────

if __name__ == "__main__":
    run_ingestion()
