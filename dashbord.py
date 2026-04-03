import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime, time

load_dotenv()

class VeloDashboard:
    def __init__(self):
        user = os.getenv("DB_USER", "postgres")
        pw = os.getenv("DB_PASSWORD", "") 
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME", "velostar_db")
        
        self.engine = create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

    def load_latest_data(self):
        """Récupère l'état le plus récent de toutes les stations pour la carte."""
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
            st.error(f"Erreur de connexion (Carte) : {e}")
            return pd.DataFrame()

    def load_historical_data(self, station_name, target_date):
        """Récupère l'historique d'une station pour une journée précise."""

        query = f"""
            SELECT f.num_bikes_available, f.collected_at
            FROM stations_status f
            JOIN stations_info s ON s.station_id = f.station_id
            WHERE s.name = '{station_name}'
              AND DATE(f.collected_at) = '{target_date}'
            ORDER BY f.collected_at ASC
        """
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            st.error(f"Erreur de connexion (Historique) : {e}")
            return pd.DataFrame()

    def run(self):
        st.set_page_config(page_title="Vélos Rennes - Expert", layout="wide")
        
        if 'selected_station' not in st.session_state:
            st.session_state.selected_station = None

        st.title("Carte en temps réel des Vélos en Libre-Service")

        df_latest = self.load_latest_data()

        if df_latest.empty:
            st.warning("La base de données semble vide ou inaccessible.")
            return

        fig = px.scatter_mapbox(
            df_latest,
            lat="lat", lon="lon",
            color="fill_rate", size="capacity",
            color_continuous_scale="RdYlGn",
            zoom=12, center={"lat": df_latest['lat'].mean(), "lon": df_latest['lon'].mean()},
            mapbox_style="carto-positron",
            custom_data=['name', 'capacity', 'num_bikes_available', 'num_docks_available', 'fill_rate']
        )

        fig.update_traces(
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                          "capacité total de %{customdata[1]} <br> " +
                          "%{customdata[2]} velos disponible <br> " +
                          "%{customdata[3]} places libre <br> " +
                          "%{customdata[4]}% de remplissage" +
                          "<extra></extra>"
        )
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=450)

        map_event = st.plotly_chart(fig, use_container_width=True, on_select="rerun", selection_mode="points")

        if map_event and len(map_event["selection"]["points"]) > 0:
            idx = map_event["selection"]["points"][0]["point_index"]
            st.session_state.selected_station = df_latest.iloc[idx]['name']

        if st.session_state.selected_station:
            st.divider()
            
            col_title, col_reset = st.columns([4, 1])
            col_title.subheader(f" Analyse de la station : {st.session_state.selected_station}")
            if col_reset.button(" Fermer l'analyse"):
                st.session_state.selected_station = None
                st.rerun()

            c1, c2 = st.columns(2)
            with c1:
                chosen_date = st.date_input("Choisir le jour :", datetime.today())
            with c2:
                time_range = st.slider(
                    "Plage horaire :",
                    value=(time(6, 0), time(22, 0)),
                    format="HH:mm"
                )

            start_time, end_time = time_range

            df_hist = self.load_historical_data(st.session_state.selected_station, chosen_date)

            if not df_hist.empty:

                df_hist['collected_at'] = pd.to_datetime(df_hist['collected_at'])
                
                df_plot = df_hist[
                    (df_hist['collected_at'].dt.time >= start_time) & 
                    (df_hist['collected_at'].dt.time <= end_time)
                ]

                if not df_plot.empty:
                    fig_line = px.line(
                        df_plot,
                        x="collected_at",
                        y="num_bikes_available",
                        markers=True,
                        title=f"Disponibilité le {chosen_date.strftime('%d/%m/%Y')} de {start_time.strftime('%H:%M')} à {end_time.strftime('%H:%M')}",
                        labels={"collected_at": "Heure", "num_bikes_available": "Vélos disponibles"}
                    )
                    
                    fig_line.update_yaxes(rangemode="tozero")
                    
                    st.plotly_chart(fig_line, use_container_width=True)
                else:
                    st.warning(" Aucune donnée pour cette plage horaire.")
            else:
                st.warning(f" Aucune donnée trouvée dans la base pour le {chosen_date.strftime('%d/%m/%Y')}.")


if __name__ == "__main__":
    app = VeloDashboard()
    app.run()