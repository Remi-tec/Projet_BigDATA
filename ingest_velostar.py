"""
=============================================================
  Ingestion API - VéloStar Rennes (GBFS / OpenDataSoft)
=============================================================
Ce script collecte les données en temps réel des stations
VéloStar de Rennes et les sauvegarde dans le Data Lake local.

Stratégie :
  Étape 1 → Appel à l'index du dataset (vls-gbfs-tr) pour
             récupérer les URLs réelles des flux GBFS.
  Étape 2 → Appel direct à chaque URL GBFS pour récupérer
             les données brutes (station_information, station_status…)

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

# Endpoint de l'index GBFS (liste des flux disponibles)
INDEX_URL = (
    "https://data.rennesmetropole.fr/api/explore/v2.1"
    "/catalog/datasets/vls-gbfs-tr/records?limit=20"
)

# Flux GBFS à collecter (correspondance idfilegbfs → nom de fichier local)
TARGET_FEEDS = {
    "station_information.json": "station_information",
    "station_status.json":      "station_status",
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


def fetch_index() -> dict:
    """
    Appelle l'index du dataset pour récupérer la liste des flux GBFS.

    Retourne un dict { idfilegbfs → filegbfsurl }
    ex: { "station_status.json": "https://eu.ftp.opendatasoft.com/..." }
    """
    logger.info("Récupération de l'index GBFS...")
    response = requests.get(INDEX_URL, timeout=15)
    response.raise_for_status()

    records = response.json().get("results", [])
    index = {r["idfilegbfs"]: r["filegbfsurl"] for r in records if "filegbfsurl" in r}

    logger.info(f"{len(index)} flux trouvés dans l'index :")
    for name, url in index.items():
        logger.info(f"   {name:35s} → {url}")

    return index


def fetch_gbfs_feed(url: str, feed_name: str) -> dict:
    """Télécharge directement un fichier GBFS depuis son URL FTP/HTTP."""
    logger.info(f"Téléchargement de '{feed_name}' depuis {url}")
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


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
            "dataset":      "vls-gbfs-tr",
        },
        "data": data,
    }


# ─────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────

def run_ingestion():
    """Lance le pipeline complet d'ingestion."""
    logger.info("=" * 55)
    logger.info("   Démarrage de l'ingestion VéloStar Rennes")
    logger.info("=" * 55)

    # 1. Créer le dossier de stockage horodaté
    folder = create_data_lake_dir()
    collected_files = []

    # 2. Récupérer l'index pour obtenir les URLs réelles
    try:
        feed_index = fetch_index()
    except requests.RequestException as e:
        logger.error(f"Impossible de récupérer l'index GBFS : {e}")
        return

    # Sauvegarder l'index brut dans le Data Lake
    index_enriched = add_metadata({"feeds": feed_index}, source=INDEX_URL)
    collected_files.append(save_to_lake(index_enriched, folder, "gbfs_index"))

    # 3. Télécharger chaque flux GBFS ciblé via son URL directe
    for feed_id, local_name in TARGET_FEEDS.items():
        if feed_id not in feed_index:
            logger.warning(f"'{feed_id}' absent de l'index, passage au suivant.")
            continue

        url = feed_index[feed_id]
        try:
            raw_data = fetch_gbfs_feed(url, feed_id)
            enriched = add_metadata(raw_data, source=url)
            path     = save_to_lake(enriched, folder, local_name)
            collected_files.append(path)
            _log_summary(raw_data, local_name)
        except requests.RequestException as e:
            logger.error(f"Erreur sur '{feed_id}' : {e}")

    # 4. Résumé final
    logger.info("-" * 55)
    logger.info(f"Ingestion terminée — {len(collected_files)} fichier(s) sauvegardé(s) :")
    for f in collected_files:
        logger.info(f"   → {f}")
    logger.info("=" * 55)

    return folder


def _log_summary(data: dict, feed_name: str):
    """Affiche un résumé rapide du contenu d'un flux GBFS."""
    try:
        items = data.get("data", {}).get("stations", [])
        if items:
            logger.info(f"   [{feed_name}] {len(items)} station(s) trouvée(s).")
    except Exception:
        pass  # résumé facultatif, ne bloque pas


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────

if __name__ == "__main__":
    run_ingestion()
