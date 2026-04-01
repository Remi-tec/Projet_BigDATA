"""
=============================================================
  Pipeline VéloStar Rennes - Architecture POO
=============================================================
Ce module implémente le pipeline complet de données VéloStar
(ingestion → transformation → chargement PostgreSQL) sous
forme de classes indépendantes et testables.

Classes :
  GBFSIngester        : collecte les données depuis l'API GBFS
  VeloStarTransformer : transforme et enrichit les données brutes
  PostgreSQLLoader    : charge les données dans PostgreSQL
  VeloStarPipeline    : orchestre les trois étapes

Utilisation :
  python pipeline_velostar.py

Tests unitaires :
  pytest test_pipeline_velostar.py
"""

import os
import glob
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

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
# GBFSIngester
# ─────────────────────────────────────────────

class GBFSIngester:
    """
    Collecte les données VéloStar depuis l'API GBFS Rennes Métropole.

    Étape 1 : récupère l'index du dataset pour obtenir les URLs réelles.
    Étape 2 : télécharge chaque flux GBFS ciblé via son URL directe.
    Étape 3 : sauvegarde les données brutes dans le Data Lake local (raw/).
    """

    INDEX_URL = (
        "https://data.rennesmetropole.fr/api/explore/v2.1"
        "/catalog/datasets/vls-gbfs-tr/records?limit=20"
    )

    TARGET_FEEDS = {
        "station_information.json": "station_information",
        "station_status.json":      "station_status",
    }

    def __init__(self, data_lake_dir: str = os.path.join("raw", "velostar")):
        self.data_lake_dir = data_lake_dir
        self.output_folder: str | None = None

    # ── Méthodes publiques ──────────────────────

    def run(self) -> str:
        """Lance l'ingestion complète et retourne le dossier de collecte."""
        logger.info("── Ingestion démarrée ──────────────────────────")
        self.output_folder = self._create_batch_folder()

        feed_index = self._fetch_index()
        self._save(
            self._add_metadata({"feeds": feed_index}, self.INDEX_URL),
            "gbfs_index",
        )

        for feed_id, local_name in self.TARGET_FEEDS.items():
            if feed_id not in feed_index:
                logger.warning(f"'{feed_id}' absent de l'index.")
                continue
            url      = feed_index[feed_id]
            raw_data = self._fetch_feed(url, feed_id)
            self._save(self._add_metadata(raw_data, url), local_name)

        logger.info(f"Ingestion terminée → {self.output_folder}")
        return self.output_folder

    def fetch_index(self) -> dict:
        """Expose la récupération de l'index (utilisé dans les tests)."""
        return self._fetch_index()

    def fetch_feed(self, url: str, name: str) -> dict:
        """Expose le téléchargement d'un flux (utilisé dans les tests)."""
        return self._fetch_feed(url, name)

    # ── Méthodes privées ────────────────────────

    def _create_batch_folder(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        folder = os.path.join(self.data_lake_dir, timestamp)
        os.makedirs(folder, exist_ok=True)
        logger.info(f"Dossier batch créé : {folder}")
        return folder

    def _fetch_index(self) -> dict:
        logger.info("Récupération de l'index GBFS...")
        response = requests.get(self.INDEX_URL, timeout=15)
        response.raise_for_status()
        records = response.json().get("results", [])
        index   = {r["idfilegbfs"]: r["filegbfsurl"] for r in records if "filegbfsurl" in r}
        logger.info(f"{len(index)} flux trouvés dans l'index.")
        return index

    def _fetch_feed(self, url: str, name: str) -> dict:
        logger.info(f"Téléchargement de '{name}'...")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()

    def _add_metadata(self, data: dict, source: str) -> dict:
        return {
            "_metadata": {
                "source":       source,
                "collected_at": datetime.now().isoformat(),
                "dataset":      "vls-gbfs-tr",
            },
            "data": data,
        }

    def _save(self, data: dict, filename: str) -> str:
        path = os.path.join(self.output_folder, f"{filename}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Sauvegardé : {path}")
        return path


# ─────────────────────────────────────────────
# VeloStarTransformer
# ─────────────────────────────────────────────

class VeloStarTransformer:
    """
    Transforme les données brutes du Data Lake en un DataFrame propre.

    Opérations :
      - Lecture et normalisation de station_information + station_status
      - Jointure sur station_id
      - Conversion des timestamps Unix en datetime UTC
      - Calcul du taux de remplissage (fill_rate)
      - Ajout du label de disponibilité
      - Export CSV dans processed/
    """

    INFO_COLS   = ["station_id", "name", "address", "post_code", "lat", "lon", "capacity"]
    STATUS_COLS = [
        "station_id", "num_bikes_available", "num_docks_available",
        "is_installed", "is_renting", "is_returning", "last_reported",
    ]
    OUTPUT_COLS = [
        "station_id", "name", "address", "post_code",
        "lat", "lon", "capacity",
        "num_bikes_available", "num_docks_available",
        "fill_rate", "availability",
        "is_installed", "is_renting", "is_returning",
        "last_reported", "collected_at",
    ]

    def __init__(
        self,
        data_lake_dir: str = os.path.join("raw", "velostar"),
        processed_dir: str = "processed",
    ):
        self.data_lake_dir = data_lake_dir
        self.processed_dir = processed_dir

    # ── Méthodes publiques ──────────────────────

    def run(self, batch_folder: str | None = None) -> tuple[pd.DataFrame, str]:
        """
        Lance la transformation complète.

        Args:
            batch_folder : chemin du batch à traiter.
                           Si None, utilise le plus récent.
        Returns:
            (DataFrame transformé, chemin du CSV exporté)
        """
        logger.info("── Transformation démarrée ─────────────────────")
        folder = batch_folder or self._get_latest_batch()

        info_wrapper   = self._load_json(folder, "station_information")
        status_wrapper = self._load_json(folder, "station_status")
        collected_at   = info_wrapper["_metadata"]["collected_at"]

        df_info   = self.build_information_df(self._extract_stations(info_wrapper))
        df_status = self.build_status_df(self._extract_stations(status_wrapper))
        df_final  = self.merge_and_enrich(df_info, df_status, collected_at)

        csv_path  = self._export_csv(df_final)
        self._log_stats(df_final)

        return df_final, csv_path

    def build_information_df(self, stations: list) -> pd.DataFrame:
        """Construit le DataFrame des informations statiques."""
        df = pd.DataFrame(stations)[self.INFO_COLS].copy()
        df["station_id"] = df["station_id"].astype(str)
        df["capacity"]   = pd.to_numeric(df["capacity"], errors="coerce").astype("Int64")
        df["lat"]        = pd.to_numeric(df["lat"],      errors="coerce")
        df["lon"]        = pd.to_numeric(df["lon"],      errors="coerce")
        df["post_code"]  = df["post_code"].astype(str)
        logger.info(f"station_information : {len(df)} stations.")
        return df

    def build_status_df(self, stations: list) -> pd.DataFrame:
        """Construit le DataFrame du statut temps réel."""
        df = pd.DataFrame(stations)[self.STATUS_COLS].copy()
        df["station_id"]          = df["station_id"].astype(str)
        df["num_bikes_available"] = pd.to_numeric(df["num_bikes_available"], errors="coerce").astype("Int64")
        df["num_docks_available"] = pd.to_numeric(df["num_docks_available"], errors="coerce").astype("Int64")
        df["is_installed"]        = df["is_installed"].astype(bool)
        df["is_renting"]          = df["is_renting"].astype(bool)
        df["is_returning"]        = df["is_returning"].astype(bool)
        df["last_reported"]       = pd.to_datetime(df["last_reported"], unit="s", utc=True)
        logger.info(f"station_status : {len(df)} stations.")
        return df

    def merge_and_enrich(
        self,
        df_info: pd.DataFrame,
        df_status: pd.DataFrame,
        collected_at: str,
    ) -> pd.DataFrame:
        """Fusionne les deux DataFrames et ajoute les colonnes calculées."""
        df = pd.merge(df_info, df_status, on="station_id", how="inner")
        df["fill_rate"]    = (df["num_bikes_available"] / df["capacity"] * 100).round(1)
        df["availability"] = df.apply(self._compute_availability, axis=1)
        df["collected_at"] = pd.to_datetime(collected_at)
        return df[self.OUTPUT_COLS]

    # ── Méthodes privées ────────────────────────

    def _get_latest_batch(self) -> str:
        batches = sorted(glob.glob(os.path.join(self.data_lake_dir, "*")))
        if not batches:
            raise FileNotFoundError(f"Aucun batch dans {self.data_lake_dir}")
        logger.info(f"Batch sélectionné : {batches[-1]}")
        return batches[-1]

    def _load_json(self, folder: str, filename: str) -> dict:
        path = os.path.join(folder, f"{filename}.json")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _extract_stations(self, wrapper: dict) -> list:
        return wrapper["data"]["data"]["stations"]

    @staticmethod
    def _compute_availability(row: pd.Series) -> str:
        if not row["is_installed"]:
            return "Hors service"
        if row["is_renting"] and row["num_bikes_available"] > 0:
            return "Disponible"
        return "Indisponible"

    def _export_csv(self, df: pd.DataFrame) -> str:
        os.makedirs(self.processed_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path      = os.path.join(self.processed_dir, f"velostar_{timestamp}.csv")
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info(f"CSV exporté : {path}")
        return path

    def _log_stats(self, df: pd.DataFrame):
        logger.info("-" * 50)
        logger.info(f"Stations totales          : {len(df)}")
        logger.info(f"Stations disponibles      : {(df['availability'] == 'Disponible').sum()}")
        logger.info(f"Stations indisponibles    : {(df['availability'] == 'Indisponible').sum()}")
        logger.info(f"Stations hors service     : {(df['availability'] == 'Hors service').sum()}")
        logger.info(f"Vélos disponibles (total) : {df['num_bikes_available'].sum()}")
        logger.info(f"Taux de remplissage moyen : {df['fill_rate'].mean():.1f}%")


# ─────────────────────────────────────────────
# PostgreSQLLoader
# ─────────────────────────────────────────────

class PostgreSQLLoader:
    """
    Charge les données transformées dans PostgreSQL.

    Tables gérées :
      - stations_info   : données statiques (UPSERT par station_id)
      - stations_status : historique temps réel (INSERT à chaque collecte)
    """

    DDL_STATIONS_INFO = """
    CREATE TABLE IF NOT EXISTS stations_info (
        station_id  VARCHAR(10)       PRIMARY KEY,
        name        VARCHAR(100)      NOT NULL,
        address     VARCHAR(200),
        post_code   VARCHAR(10),
        lat         DOUBLE PRECISION,
        lon         DOUBLE PRECISION,
        capacity    INTEGER,
        updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    """

    DDL_STATIONS_STATUS = """
    CREATE TABLE IF NOT EXISTS stations_status (
        id                   SERIAL PRIMARY KEY,
        station_id           VARCHAR(10),
        num_bikes_available  INTEGER,
        num_docks_available  INTEGER,
        fill_rate            NUMERIC(5,1),
        availability         VARCHAR(20),
        is_installed         BOOLEAN,
        is_renting           BOOLEAN,
        is_returning         BOOLEAN,
        last_reported        TIMESTAMP WITH TIME ZONE,
        collected_at         TIMESTAMP WITH TIME ZONE,
        FOREIGN KEY (station_id) REFERENCES stations_info(station_id)
    );
    """

    UPSERT_INFO_SQL = text("""
        INSERT INTO stations_info
            (station_id, name, address, post_code, lat, lon, capacity, updated_at)
        VALUES
            (:station_id, :name, :address, :post_code, :lat, :lon, :capacity, :updated_at)
        ON CONFLICT (station_id) DO UPDATE SET
            name       = EXCLUDED.name,
            address    = EXCLUDED.address,
            post_code  = EXCLUDED.post_code,
            lat        = EXCLUDED.lat,
            lon        = EXCLUDED.lon,
            capacity   = EXCLUDED.capacity,
            updated_at = EXCLUDED.updated_at;
    """)

    def __init__(
        self,
        host:     str = os.getenv("DB_HOST",     "localhost"),
        port:     str = os.getenv("DB_PORT",     "5432"),
        dbname:   str = os.getenv("DB_NAME",     "velostar_db"),
        user:     str = os.getenv("DB_USER",     "postgres"),
        password: str = os.getenv("DB_PASSWORD", ""),
        processed_dir: str = "processed",
    ):
        self.processed_dir = processed_dir
        self._engine = self._create_engine(host, port, dbname, user, password)

    # ── Méthodes publiques ──────────────────────

    def run(self, csv_path: str | None = None) -> None:
        """Lance le chargement complet."""
        logger.info("── Chargement PostgreSQL démarré ───────────────")
        self.create_tables()

        path = csv_path or self._get_latest_csv()
        df   = pd.read_csv(path, parse_dates=["last_reported", "collected_at"])
        logger.info(f"CSV chargé : {len(df)} lignes.")

        self.load_stations_info(df)
        self.load_stations_status(df)
        self._log_counts()

    def create_tables(self) -> None:
        """Crée les tables si elles n'existent pas encore."""
        with self._engine.connect() as conn:
            conn.execute(text(self.DDL_STATIONS_INFO))
            conn.execute(text(self.DDL_STATIONS_STATUS))
            conn.commit()
        logger.info("Tables vérifiées / créées.")

    def load_stations_info(self, df: pd.DataFrame) -> None:
        """UPSERT des données statiques dans stations_info."""
        cols    = ["station_id", "name", "address", "post_code", "lat", "lon", "capacity"]
        df_info = df[cols].drop_duplicates("station_id").copy()
        df_info["updated_at"] = datetime.now()

        with self._engine.connect() as conn:
            conn.execute(self.UPSERT_INFO_SQL, df_info.to_dict(orient="records"))
            conn.commit()
        logger.info(f"stations_info : {len(df_info)} ligne(s) insérées/mises à jour.")

    def load_stations_status(self, df: pd.DataFrame) -> None:
        """INSERT du snapshot temps réel dans stations_status."""
        cols = [
            "station_id", "num_bikes_available", "num_docks_available",
            "fill_rate", "availability",
            "is_installed", "is_renting", "is_returning",
            "last_reported", "collected_at",
        ]
        df[cols].to_sql(
            "stations_status", self._engine,
            if_exists="append", index=False, method="multi",
        )
        logger.info(f"stations_status : {len(df)} snapshot(s) insérés.")

    def count(self, table: str) -> int:
        """Retourne le nombre de lignes d'une table (utile pour les tests)."""
        with self._engine.connect() as conn:
            return conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()

    # ── Méthodes privées ────────────────────────

    @staticmethod
    def _create_engine(host, port, dbname, user, password):
        url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(url)
        logger.info(f"Moteur SQLAlchemy → {host}:{port}/{dbname}")
        return engine

    def _get_latest_csv(self) -> str:
        files = sorted(glob.glob(os.path.join(self.processed_dir, "velostar_*.csv")))
        if not files:
            raise FileNotFoundError(f"Aucun CSV dans {self.processed_dir}/")
        logger.info(f"CSV sélectionné : {files[-1]}")
        return files[-1]

    def _log_counts(self):
        logger.info("-" * 50)
        logger.info(f"stations_info   : {self.count('stations_info')} station(s)")
        logger.info(f"stations_status : {self.count('stations_status')} snapshot(s)")


# ─────────────────────────────────────────────
# VeloStarPipeline  (orchestrateur)
# ─────────────────────────────────────────────

class VeloStarPipeline:
    """
    Orchestre les trois étapes du pipeline dans l'ordre :
      GBFSIngester → VeloStarTransformer → PostgreSQLLoader
    """

    def __init__(
        self,
        data_lake_dir: str = os.path.join("raw", "velostar"),
        processed_dir: str = "processed",
    ):
        self.ingester    = GBFSIngester(data_lake_dir=data_lake_dir)
        self.transformer = VeloStarTransformer(
            data_lake_dir=data_lake_dir,
            processed_dir=processed_dir,
        )
        self.loader      = PostgreSQLLoader(processed_dir=processed_dir)

    def run(self) -> None:
        """Exécute le pipeline complet."""
        logger.info("═" * 55)
        logger.info("   Pipeline VéloStar Rennes — démarrage")
        logger.info("═" * 55)

        # Étape 1 — Ingestion
        batch_folder = self.ingester.run()

        # Étape 2 — Transformation
        _, csv_path = self.transformer.run(batch_folder=batch_folder)

        # Étape 3 — Chargement
        self.loader.run(csv_path=csv_path)

        logger.info("═" * 55)
        logger.info("   Pipeline terminé avec succès ✓")
        logger.info("═" * 55)


# ─────────────────────────────────────────────
# Point d'entrée
# ─────────────────────────────────────────────

if __name__ == "__main__":
    VeloStarPipeline().run()
