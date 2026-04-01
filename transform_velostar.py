"""
=============================================================
  Transformation - VéloStar Rennes
=============================================================
Ce script lit les données brutes du Data Lake, les transforme
avec pandas et produit un fichier CSV propre prêt à être
chargé dans PostgreSQL.

Opérations effectuées :
  - Lecture des JSON bruts (station_information + station_status)
  - Normalisation et typage des colonnes
  - Jointure sur station_id
  - Conversion des timestamps Unix → datetime lisible
  - Calcul du taux de remplissage (%)
  - Ajout d'une colonne statut lisible (Disponible / Indisponible)
  - Export CSV dans processed/

Input  : raw/velostar/<timestamp>/station_information.json
                                  station_status.json
Output : processed/velostar_<timestamp>.csv
"""

import os
import json
import glob
import logging
import pandas as pd
from datetime import datetime, timezone

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

DATA_LAKE_DIR  = os.path.join("raw", "velostar")
PROCESSED_DIR  = "processed"

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

def get_latest_batch() -> str:
    """Retourne le dossier de la collecte la plus récente."""
    batches = sorted(glob.glob(os.path.join(DATA_LAKE_DIR, "*")))
    if not batches:
        raise FileNotFoundError(f"Aucun batch trouvé dans {DATA_LAKE_DIR}")
    latest = batches[-1]
    logger.info(f"Batch sélectionné : {latest}")
    return latest


def load_json(folder: str, filename: str) -> dict:
    """Charge un fichier JSON du Data Lake."""
    path = os.path.join(folder, f"{filename}.json")
    with open(path, "r", encoding="utf-8") as f:
        wrapper = json.load(f)
    # Les données sont imbriquées : wrapper["data"]["data"]["stations"]
    return wrapper


def extract_stations(wrapper: dict) -> list:
    """Extrait la liste des stations depuis la structure GBFS imbriquée."""
    return wrapper["data"]["data"]["stations"]


def extract_last_updated(wrapper: dict) -> int:
    """Extrait le timestamp last_updated du flux GBFS."""
    return wrapper["data"]["last_updated"]


def unix_to_datetime(ts: int) -> datetime:
    """Convertit un timestamp Unix (secondes) en datetime UTC."""
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# ─────────────────────────────────────────────
# Transformations
# ─────────────────────────────────────────────

def build_information_df(stations: list) -> pd.DataFrame:
    """
    Construit le DataFrame des informations statiques des stations.

    Colonnes retenues :
      station_id, name, address, post_code, lat, lon, capacity
    """
    df = pd.DataFrame(stations)

    # Colonnes utiles uniquement
    cols = ["station_id", "name", "address", "post_code", "lat", "lon", "capacity"]
    df = df[cols].copy()

    # Typage
    df["station_id"] = df["station_id"].astype(str)
    df["capacity"]   = pd.to_numeric(df["capacity"], errors="coerce").astype("Int64")
    df["lat"]        = pd.to_numeric(df["lat"],      errors="coerce")
    df["lon"]        = pd.to_numeric(df["lon"],      errors="coerce")
    df["post_code"]  = df["post_code"].astype(str)

    logger.info(f"station_information : {len(df)} stations chargées.")
    return df


def build_status_df(stations: list) -> pd.DataFrame:
    """
    Construit le DataFrame du statut temps réel des stations.

    Colonnes retenues :
      station_id, num_bikes_available, num_docks_available,
      is_installed, is_renting, is_returning, last_reported
    """
    df = pd.DataFrame(stations)

    cols = [
        "station_id", "num_bikes_available", "num_docks_available",
        "is_installed", "is_renting", "is_returning", "last_reported",
    ]
    df = df[cols].copy()

    # Typage
    df["station_id"]           = df["station_id"].astype(str)
    df["num_bikes_available"]  = pd.to_numeric(df["num_bikes_available"],  errors="coerce").astype("Int64")
    df["num_docks_available"]  = pd.to_numeric(df["num_docks_available"],  errors="coerce").astype("Int64")
    df["is_installed"]         = df["is_installed"].astype(bool)
    df["is_renting"]           = df["is_renting"].astype(bool)
    df["is_returning"]         = df["is_returning"].astype(bool)

    # Conversion timestamp Unix → datetime UTC
    df["last_reported"] = pd.to_datetime(
        df["last_reported"], unit="s", utc=True
    )

    logger.info(f"station_status : {len(df)} stations chargées.")
    return df


def merge_and_enrich(df_info: pd.DataFrame, df_status: pd.DataFrame, collected_at: str) -> pd.DataFrame:
    """
    Fusionne les deux DataFrames sur station_id et enrichit le résultat.

    Colonnes ajoutées :
      - fill_rate      : taux de remplissage (vélos / capacité * 100)
      - availability   : label lisible (Disponible / Indisponible / Hors service)
      - collected_at   : horodatage de la collecte
    """
    df = pd.merge(df_info, df_status, on="station_id", how="inner")
    logger.info(f"Jointure effectuée : {len(df)} stations après merge.")

    # Taux de remplissage (en %)
    df["fill_rate"] = (
        df["num_bikes_available"] / df["capacity"] * 100
    ).round(1)

    # Statut lisible
    def compute_availability(row):
        if not row["is_installed"]:
            return "Hors service"
        if row["is_renting"] and row["num_bikes_available"] > 0:
            return "Disponible"
        return "Indisponible"

    df["availability"] = df.apply(compute_availability, axis=1)

    # Horodatage de la collecte
    df["collected_at"] = pd.to_datetime(collected_at)

    # Ordre logique des colonnes
    ordered_cols = [
        "station_id", "name", "address", "post_code",
        "lat", "lon", "capacity",
        "num_bikes_available", "num_docks_available",
        "fill_rate", "availability",
        "is_installed", "is_renting", "is_returning",
        "last_reported", "collected_at",
    ]
    df = df[ordered_cols]

    return df


# ─────────────────────────────────────────────
# Pipeline principal
# ─────────────────────────────────────────────

def run_transformation():
    """Lance le pipeline complet de transformation."""
    logger.info("=" * 55)
    logger.info("   Démarrage de la transformation VéloStar")
    logger.info("=" * 55)

    # 1. Trouver le batch le plus récent
    folder = get_latest_batch()

    # 2. Charger les fichiers bruts
    info_wrapper   = load_json(folder, "station_information")
    status_wrapper = load_json(folder, "station_status")

    collected_at = info_wrapper["_metadata"]["collected_at"]
    logger.info(f"Données collectées le : {collected_at}")

    # 3. Extraire les listes de stations
    info_stations   = extract_stations(info_wrapper)
    status_stations = extract_stations(status_wrapper)

    # 4. Construire les DataFrames
    df_info   = build_information_df(info_stations)
    df_status = build_status_df(status_stations)

    # 5. Fusionner et enrichir
    df_final = merge_and_enrich(df_info, df_status, collected_at)

    # 6. Aperçu console
    logger.info("\n" + df_final[["station_id", "name", "num_bikes_available", "fill_rate", "availability"]].to_string(index=False))

    # 7. Export CSV dans processed/
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = os.path.join(PROCESSED_DIR, f"velostar_{timestamp}.csv")
    df_final.to_csv(output_csv, index=False, encoding="utf-8")
    logger.info(f"\nCSV exporté : {output_csv}")

    # 8. Statistiques rapides
    logger.info("-" * 55)
    logger.info(f"Stations totales          : {len(df_final)}")
    logger.info(f"Stations disponibles      : {(df_final['availability'] == 'Disponible').sum()}")
    logger.info(f"Stations indisponibles    : {(df_final['availability'] == 'Indisponible').sum()}")
    logger.info(f"Stations hors service     : {(df_final['availability'] == 'Hors service').sum()}")
    logger.info(f"Vélos disponibles (total) : {df_final['num_bikes_available'].sum()}")
    logger.info(f"Taux de remplissage moyen : {df_final['fill_rate'].mean():.1f}%")
    logger.info("=" * 55)

    return df_final, output_csv


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────

if __name__ == "__main__":
    run_transformation()
