import pandas as pd
from sqlalchemy import create_engine, text

class PostgresModeler:
    def __init__(self, user, password, host, port, dbname):
        self.url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        self.engine = create_engine(self.url)

    def create_schema(self):
        """Crée les tables avec les bons types de données."""
        with self.engine.connect() as conn:
            # Table des stations (statique)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dim_stations (
                    station_id INT PRIMARY KEY,
                    name TEXT,
                    address TEXT,
                    post_code VARCHAR(10),
                    lat FLOAT,
                    lon FLOAT,
                    capacity INT
                );
            """))
            # Table des mesures (temporelle)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fact_station_status (
                    id SERIAL PRIMARY KEY,
                    station_id INT REFERENCES dim_stations(station_id),
                    num_bikes_available INT,
                    num_docks_available INT,
                    fill_rate FLOAT,
                    availability TEXT,
                    is_renting BOOLEAN,
                    last_reported TIMESTAMPTZ,
                    collected_at TIMESTAMPTZ
                );
            """))
            conn.commit()
            print(" Schéma PostgreSQL prêt (Star Schema).")

    def insert_data(self, df: pd.DataFrame):
        """Sépare et insère les données dans les deux tables."""
        stations_df = df[['station_id', 'name', 'address', 'post_code', 'lat', 'lon', 'capacity']].drop_duplicates()
        stations_df.to_sql('dim_stations', self.engine, if_exists='append', index=False, method='multi')
        
        status_df = df[['station_id', 'num_bikes_available', 'num_docks_available', 
                        'fill_rate', 'availability', 'is_renting', 'last_reported', 'collected_at']]
        status_df.to_sql('fact_station_status', self.engine, if_exists='append', index=False)
        print(f" {len(df)} enregistrements insérés en base.")