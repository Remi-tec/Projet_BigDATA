import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

class VeloDashboard:
    def __init__(self):
        user = os.getenv("DB_USER", "postgres")
        pw = os.getenv("DB_PASSWORD", "")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME", "velostar_db")
        
        self.engine = create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

    def load_data(self):
        """Récupèration des dernières données connues pour chaque station"""
        query = """
            SELECT s.station_id, s.name, s.lat, s.lon, s.capacity,
                   f.num_bikes_available, f.num_docks_available, f.fill_rate
                 FROM stations_info s
                 JOIN stations_status f ON s.station_id = f.station_id
                 WHERE f.collected_at = (SELECT MAX(collected_at) FROM stations_status)
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            st.error(f"Erreur de connexion à la base de données : {e}")
            return pd.DataFrame() 

    def run(self):
        st.set_page_config(page_title="Carte Vélos Rennes", layout="wide")
        st.title("🚲 Carte en temps réel des Vélos en Libre-Service")

        # Chargement des données
        df = self.load_data()

        if df.empty:
            st.warning("Aucune donnée trouvée dans la base. ") 
            return

        
        centre_lat = df['lat'].mean()
        centre_lon = df['lon'].mean()

        fig = px.scatter_mapbox(
            df,
            lat="lat",
            lon="lon",
            hover_name="name", 
            hover_data={
                "lat": False, 
                "lon": False,
                "num_bikes_available": True, # Affiche les vélos disponibles
                "num_docks_available": True, # Affiche les places libres
                "fill_rate": True
            },
            labels={
                "capacity": "Capacité totale",
                "num_bikes_available": "Vélos disponibles",
                "num_docks_available": "Places libres",
                "fill_rate": "Remplissage (%)"
            },
            color="fill_rate", 
            color_continuous_scale="RdYlGn", 
            size="capacity", 
            zoom=12,
            center={"lat": centre_lat, "lon": centre_lon},
            mapbox_style="carto-positron", 
            title="Disponibilité par station"
        )

        fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})

        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    app = VeloDashboard()
    app.run()