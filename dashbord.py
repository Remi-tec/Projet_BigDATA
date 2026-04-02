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

    def load_map_data(self):
        """Données pour la carte (Dernier état)"""
        query = """
            SELECT s.station_id, s.name, s.lat, s.lon, s.capacity,
                   f.num_bikes_available, f.num_docks_available, f.fill_rate
            FROM stations_info s
            JOIN stations_status f ON s.station_id = f.station_id
            WHERE f.collected_at = (SELECT MAX(collected_at) FROM stations_status)
        """
        return pd.read_sql(query, self.engine)

    def load_station_history(self, station_name):
        """Récupère l'historique d'une station précise sur les dernières 24h"""
        query = f"""
            SELECT f.num_bikes_available, f.collected_at
            FROM stations_status f
            JOIN stations_info s ON s.station_id = f.station_id
            WHERE s.name = '{station_name}'
            ORDER BY f.collected_at ASC
        """
        return pd.read_sql(query, self.engine)

    def run(self):
        st.set_page_config(page_title="Rennes Vélos", layout="wide")
        st.title("🚲 Carte en temps réel des Vélos en Libre-Service")

        df_map = self.load_map_data()

        if df_map.empty:
            st.error("Base de données vide.")
            return

        fig = px.scatter_mapbox(
            df_map,
            lat="lat",
            lon="lon",
            color="fill_rate",
            size="capacity",
            color_continuous_scale="RdYlGn",
            zoom=12,
            center={"lat": df_map['lat'].mean(), "lon": df_map['lon'].mean()},
            mapbox_style="carto-positron",
            custom_data=['name', 'capacity', 'num_bikes_available', 'num_docks_available', 'fill_rate']
        )

        fig.update_traces(
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                          "capacité total de %{customdata[1]} <br> " +
                          "%{customdata[2]} velos disponible <br>" +
                          "%{customdata[3]} places libre <br> " +
                          "%{customdata[4]}% de remplissage" +
                          "<extra></extra>"
        )
        
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=500)

        event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode="points")

        if event and len(event["selection"]["points"]) > 0:

            point_index = event["selection"]["points"][0]["point_index"]
            station_cliquee = df_map.iloc[point_index]['name']

            st.divider()
            st.subheader(f"📈 Évolution à la station : {station_cliquee}")
            
            df_hist = self.load_station_history(station_cliquee)

            if not df_hist.empty:
                fig_line = px.line(
                    df_hist, 
                    x="collected_at", 
                    y="num_bikes_available",
                    markers=True,
                    labels={"collected_at": "Heure", "num_bikes_available": "Vélos disponibles"}
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info("Pas d'historique pour cette station.")
        else:
            st.info("💡 Cliquez sur un point coloré de la carte pour afficher le graphique historique de cette station.")

if __name__ == "__main__":
    app = VeloDashboard()
    app.run()
    