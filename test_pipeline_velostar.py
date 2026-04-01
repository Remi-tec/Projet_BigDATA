"""
=============================================================
  Tests unitaires - Pipeline VéloStar Rennes
=============================================================
Chaque classe est testée indépendamment via des mocks,
sans connexion réseau ni base de données réelle.

Lancer les tests :
  pytest test_pipeline_velostar.py -v
"""

import json
import os
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, mock_open
from pipeline_velostar import GBFSIngester, VeloStarTransformer, PostgreSQLLoader


# ─────────────────────────────────────────────
# Fixtures partagées
# ─────────────────────────────────────────────

FAKE_INDEX_RESPONSE = {
    "results": [
        {
            "idfilegbfs":      "station_information.json",
            "filegbfsurl":     "https://fake.url/station_information.json",
            "filegbfsrequired":"Requis",
        },
        {
            "idfilegbfs":      "station_status.json",
            "filegbfsurl":     "https://fake.url/station_status.json",
            "filegbfsrequired":"Requis sous condition",
        },
    ]
}

FAKE_STATION_INFO = {
    "last_updated": 1775036768,
    "ttl": 60,
    "data": {
        "stations": [
            {
                "station_id": "5501", "name": "République",
                "address": "19 Quai Lamartine", "post_code": "35238",
                "lat": 48.110026, "lon": -1.678037, "capacity": 45,
                "rental_methods": ["KEY"],
            },
            {
                "station_id": "5502", "name": "Mairie - Opéra",
                "address": "11 galeries du Théâtre", "post_code": "35238",
                "lat": 48.111624, "lon": -1.678757, "capacity": 24,
                "rental_methods": ["KEY"],
            },
        ]
    },
}

FAKE_STATION_STATUS = {
    "last_updated": 1775036768,
    "ttl": 60,
    "data": {
        "stations": [
            {
                "station_id": "5501", "num_bikes_available": 18,
                "num_docks_available": 27, "is_installed": 1,
                "is_renting": 1, "is_returning": 1,
                "last_reported": 1775036715,
            },
            {
                "station_id": "5502", "num_bikes_available": 0,
                "num_docks_available": 8, "is_installed": 1,
                "is_renting": 1, "is_returning": 1,
                "last_reported": 1775036715,
            },
        ]
    },
}

FAKE_WRAPPER_INFO = {
    "_metadata": {
        "source":       "https://fake.url/station_information.json",
        "collected_at": "2026-04-01T11:46:21.127290",
        "dataset":      "vls-gbfs-tr",
    },
    "data": FAKE_STATION_INFO,
}

FAKE_WRAPPER_STATUS = {
    "_metadata": {
        "source":       "https://fake.url/station_status.json",
        "collected_at": "2026-04-01T11:46:21.552417",
        "dataset":      "vls-gbfs-tr",
    },
    "data": FAKE_STATION_STATUS,
}


# ─────────────────────────────────────────────
# Tests — GBFSIngester
# ─────────────────────────────────────────────

class TestGBFSIngester:

    def setup_method(self):
        self.ingester = GBFSIngester(data_lake_dir="raw/test")

    @patch("pipeline_velostar.requests.get")
    def test_fetch_index_returns_dict(self, mock_get):
        """fetch_index doit retourner un dict {nom_flux: url}."""
        mock_get.return_value.json.return_value = FAKE_INDEX_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()

        result = self.ingester.fetch_index()

        assert isinstance(result, dict)
        assert "station_information.json" in result
        assert result["station_information.json"] == "https://fake.url/station_information.json"

    @patch("pipeline_velostar.requests.get")
    def test_fetch_index_correct_number_of_feeds(self, mock_get):
        """fetch_index doit retourner autant d'entrées que de résultats avec une URL."""
        mock_get.return_value.json.return_value = FAKE_INDEX_RESPONSE
        mock_get.return_value.raise_for_status = MagicMock()

        result = self.ingester.fetch_index()
        assert len(result) == 2

    @patch("pipeline_velostar.requests.get")
    def test_fetch_feed_returns_json(self, mock_get):
        """fetch_feed doit retourner le contenu JSON du flux."""
        mock_get.return_value.json.return_value = FAKE_STATION_INFO
        mock_get.return_value.raise_for_status = MagicMock()

        result = self.ingester.fetch_feed("https://fake.url/station_information.json", "station_information.json")

        assert "data" in result
        assert "stations" in result["data"]

    @patch("pipeline_velostar.requests.get")
    def test_fetch_index_network_error(self, mock_get):
        """fetch_index doit propager l'exception réseau."""
        mock_get.side_effect = Exception("Timeout")
        with pytest.raises(Exception, match="Timeout"):
            self.ingester.fetch_index()


# ─────────────────────────────────────────────
# Tests — VeloStarTransformer
# ─────────────────────────────────────────────

class TestVeloStarTransformer:

    def setup_method(self):
        self.transformer = VeloStarTransformer()
        self.stations_info   = FAKE_STATION_INFO["data"]["stations"]
        self.stations_status = FAKE_STATION_STATUS["data"]["stations"]

    def test_build_information_df_shape(self):
        """build_information_df doit retourner autant de lignes que de stations."""
        df = self.transformer.build_information_df(self.stations_info)
        assert len(df) == 2

    def test_build_information_df_columns(self):
        """build_information_df doit contenir les colonnes attendues."""
        df = self.transformer.build_information_df(self.stations_info)
        for col in ["station_id", "name", "lat", "lon", "capacity"]:
            assert col in df.columns

    def test_build_information_df_types(self):
        """Les colonnes numériques doivent avoir le bon type."""
        df = self.transformer.build_information_df(self.stations_info)
        assert pd.api.types.is_float_dtype(df["lat"])
        assert pd.api.types.is_float_dtype(df["lon"])

    def test_build_status_df_shape(self):
        """build_status_df doit retourner autant de lignes que de stations."""
        df = self.transformer.build_status_df(self.stations_status)
        assert len(df) == 2

    def test_build_status_df_last_reported_is_datetime(self):
        """last_reported doit être converti en datetime."""
        df = self.transformer.build_status_df(self.stations_status)
        assert pd.api.types.is_datetime64_any_dtype(df["last_reported"])

    def test_merge_and_enrich_fill_rate(self):
        """fill_rate doit être = num_bikes_available / capacity * 100."""
        df_info   = self.transformer.build_information_df(self.stations_info)
        df_status = self.transformer.build_status_df(self.stations_status)
        df        = self.transformer.merge_and_enrich(df_info, df_status, "2026-04-01T11:46:21")

        row_5501 = df[df["station_id"] == "5501"].iloc[0]
        expected = round(18 / 45 * 100, 1)
        assert row_5501["fill_rate"] == expected

    def test_merge_and_enrich_availability_disponible(self):
        """Une station avec des vélos doit être 'Disponible'."""
        df_info   = self.transformer.build_information_df(self.stations_info)
        df_status = self.transformer.build_status_df(self.stations_status)
        df        = self.transformer.merge_and_enrich(df_info, df_status, "2026-04-01T11:46:21")

        row_5501 = df[df["station_id"] == "5501"].iloc[0]
        assert row_5501["availability"] == "Disponible"

    def test_merge_and_enrich_availability_indisponible(self):
        """Une station sans vélos mais active doit être 'Indisponible'."""
        df_info   = self.transformer.build_information_df(self.stations_info)
        df_status = self.transformer.build_status_df(self.stations_status)
        df        = self.transformer.merge_and_enrich(df_info, df_status, "2026-04-01T11:46:21")

        row_5502 = df[df["station_id"] == "5502"].iloc[0]
        assert row_5502["availability"] == "Indisponible"

    def test_merge_and_enrich_hors_service(self):
        """Une station avec is_installed=False doit être 'Hors service'."""
        status_hors_service = [
            {**self.stations_status[0], "is_installed": 0, "is_renting": 0, "is_returning": 0}
        ]
        info_one  = [self.stations_info[0]]
        df_info   = self.transformer.build_information_df(info_one)
        df_status = self.transformer.build_status_df(status_hors_service)
        df        = self.transformer.merge_and_enrich(df_info, df_status, "2026-04-01T11:46:21")

        assert df.iloc[0]["availability"] == "Hors service"

    def test_merge_and_enrich_output_columns(self):
        """Le DataFrame final doit contenir exactement les colonnes OUTPUT_COLS."""
        df_info   = self.transformer.build_information_df(self.stations_info)
        df_status = self.transformer.build_status_df(self.stations_status)
        df        = self.transformer.merge_and_enrich(df_info, df_status, "2026-04-01T11:46:21")

        assert list(df.columns) == VeloStarTransformer.OUTPUT_COLS

    def test_merge_joins_on_station_id(self):
        """La jointure ne doit garder que les station_id communs aux deux DataFrames."""
        # On retire la station 5502 du status → la jointure doit donner 1 ligne
        df_info   = self.transformer.build_information_df(self.stations_info)
        df_status = self.transformer.build_status_df([self.stations_status[0]])
        df        = self.transformer.merge_and_enrich(df_info, df_status, "2026-04-01T11:46:21")

        assert len(df) == 1
        assert df.iloc[0]["station_id"] == "5501"


# ─────────────────────────────────────────────
# Tests — PostgreSQLLoader
# ─────────────────────────────────────────────

class TestPostgreSQLLoader:

    def _make_loader(self, mock_engine):
        """Instancie un loader avec un moteur mocké."""
        loader = PostgreSQLLoader.__new__(PostgreSQLLoader)
        loader.processed_dir = "processed"
        loader._engine       = mock_engine
        return loader

    def test_load_stations_info_calls_execute(self):
        """load_stations_info doit exécuter une requête SQL."""
        mock_engine = MagicMock()
        loader      = self._make_loader(mock_engine)

        df = pd.DataFrame([{
            "station_id": "5501", "name": "République",
            "address": "19 Quai Lamartine", "post_code": "35238",
            "lat": 48.11, "lon": -1.67, "capacity": 45,
        }])
        loader.load_stations_info(df)

        mock_engine.connect().__enter__().execute.assert_called_once()

    def test_load_stations_status_uses_append(self):
        """load_stations_status doit utiliser to_sql avec if_exists='append'."""
        mock_engine = self._make_loader(MagicMock())._engine
        loader      = self._make_loader(mock_engine)

        df = pd.DataFrame([{
            "station_id": "5501",
            "num_bikes_available": 18, "num_docks_available": 27,
            "fill_rate": 40.0, "availability": "Disponible",
            "is_installed": True, "is_renting": True, "is_returning": True,
            "last_reported": pd.Timestamp("2026-04-01"), "collected_at": pd.Timestamp("2026-04-01"),
        }])

        with patch.object(df.__class__, "to_sql") as mock_to_sql:
            loader.load_stations_status(df)
            mock_to_sql.assert_called_once()
            _, kwargs = mock_to_sql.call_args
            assert kwargs.get("if_exists") == "append"

    def test_count_returns_integer(self):
        """count() doit retourner un entier."""
        mock_engine = MagicMock()
        mock_engine.connect().__enter__().execute().scalar.return_value = 42
        loader = self._make_loader(mock_engine)

        result = loader.count("stations_info")
        assert isinstance(result, int)
