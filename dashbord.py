import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime, time, timedelta
import pytz
from shapely.geometry import Point, Polygon

load_dotenv()

class VeloDashboard:
    def __init__(self):
        user = os.getenv("DB_USER", "postgres")
        pw = os.getenv("DB_PASSWORD", "")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME", "velostar_db")
        
        self.engine = create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")
        
        # Définition des périmètres des quartiers avec des polygones (coordonnées lat, lon)
        self.quartier_polygons = {
            "Centre-Ville": Polygon([
                (48.1085, -1.6820), (48.1135, -1.6820), 
                (48.1135, -1.6670), (48.1085, -1.6670)
            ]),
            "Gare": Polygon([
                (48.0975, -1.6880), (48.1105, -1.6880), 
                (48.1105, -1.6680), (48.0975, -1.6680)
            ]),
            "Sainte-Thérèse": Polygon([
                (48.0920, -1.7050), (48.1090, -1.7050), 
                (48.1090, -1.6750), (48.0920, -1.6750)
            ]),
            "Beaulieu": Polygon([
                (48.0890, -1.6550), (48.1080, -1.6550), 
                (48.1080, -1.6250), (48.0890, -1.6250)
            ]),
            "Saint-Héller": Polygon([
                (48.1100, -1.7000), (48.1300, -1.7000), 
                (48.1300, -1.6700), (48.1100, -1.6700)
            ]),
            "Bourg l'Évêque": Polygon([
                (48.1050, -1.7200), (48.1250, -1.7200), 
                (48.1250, -1.6850), (48.1050, -1.6850)
            ]),
            "Jeanne d'Arc": Polygon([
                (48.0800, -1.7100), (48.0980, -1.7100), 
                (48.0980, -1.6700), (48.0800, -1.6700)
            ]),
            "Cleunay": Polygon([
                (48.1250, -1.7350), (48.1500, -1.7350), 
                (48.1500, -1.6850), (48.1250, -1.6850)
            ]),
            "Arsenal/Redon": Polygon([
                (48.1000, -1.6400), (48.1250, -1.6400), 
                (48.1250, -1.6000), (48.1000, -1.6000)
            ]),
        }

    def fix_timezone(self, df):
        """Convertit les dates UTC de la DB en heure locale Europe/Paris (UTC+2)"""
        if not df.empty and 'collected_at' in df.columns:
            df['collected_at'] = pd.to_datetime(df['collected_at'])

            if df['collected_at'].dt.tz is None:
                df['collected_at'] = df['collected_at'].dt.tz_localize('UTC')

            df['collected_at'] = df['collected_at'].dt.tz_convert('Europe/Paris').dt.tz_localize(None)
        return df

    def get_all_stations(self):
        """Récupère tous les noms de stations de la BD"""
        query = "SELECT DISTINCT name FROM stations_info ORDER BY name"
        df = pd.read_sql(query, self.engine)
        return df['name'].tolist() if not df.empty else []

    def get_quartier_from_coordinates(self, lat, lon):
        """Détermine le quartier basé sur les coordonnées (lat, lon) de la station
        en utilisant des polygones pour délimiter les périmètres"""
        point = Point(lat, lon)
        for quartier, polygon in self.quartier_polygons.items():
            if polygon.contains(point):
                return quartier
        return "Inconnu"  # Si la station est en dehors de tous les quartiers définis

    def load_latest_data(self):
        """Données pour la carte (Dernier état connu)"""
        query = """
            SELECT s.station_id, s.name, s.lat, s.lon, s.capacity,
                   f.num_bikes_available, f.num_docks_available, f.fill_rate, f.collected_at
            FROM stations_info s
            JOIN stations_status f ON s.station_id = f.station_id
            WHERE f.collected_at = (SELECT MAX(collected_at) FROM stations_status)
        """
        df = pd.read_sql(query, self.engine)
        df = self.fix_timezone(df)
        
        # Ajouter la colonne quartier basée sur les coordonnées de la station
        df['quartier'] = df.apply(lambda row: self.get_quartier_from_coordinates(row['lat'], row['lon']), axis=1)
        
        return df

    def load_weekly_data(self, station_name, target_date):
        """Récupère l'historique de la station sur la semaine de la date choisie"""
        start_of_week = target_date - timedelta(days=target_date.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        
        query = f"""
            SELECT f.num_bikes_available, f.fill_rate, f.collected_at, s.lat, s.lon
            FROM stations_status f
            JOIN stations_info s ON s.station_id = f.station_id
            WHERE s.name = '{station_name}'
              AND DATE(f.collected_at) >= '{start_of_week}'
              AND DATE(f.collected_at) <= '{end_of_week}'
            ORDER BY f.collected_at ASC
        """
        df = pd.read_sql(query, self.engine)
        df = self.fix_timezone(df)
        
        # Ajouter la colonne quartier basée sur les coordonnées
        if not df.empty:
            lat, lon = df['lat'].iloc[0], df['lon'].iloc[0]
            df['quartier'] = self.get_quartier_from_coordinates(lat, lon)
            df = df.drop(['lat', 'lon'], axis=1)
        
        return df

    def load_quartier_data(self, quartier, target_date):
        """Récupère l'historique du quartier sur la semaine de la date choisie"""
        start_of_week = target_date - timedelta(days=target_date.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        
        # Récupérer le polygone du quartier
        polygon = self.quartier_polygons.get(quartier)
        if not polygon:
            return pd.DataFrame()
        
        # Récupérer les limites du polygone (bounds)
        minx, miny, maxx, maxy = polygon.bounds
        
        # Trouver toutes les stations dans cette zone de quartier
        query = f"""
            SELECT f.num_bikes_available, f.fill_rate, f.collected_at, s.lat, s.lon
            FROM stations_status f
            JOIN stations_info s ON s.station_id = f.station_id
            WHERE s.lat >= {miny} AND s.lat <= {maxx}
              AND s.lon >= {minx} AND s.lon <= {maxy}
              AND DATE(f.collected_at) >= '{start_of_week}'
              AND DATE(f.collected_at) <= '{end_of_week}'
            ORDER BY f.collected_at ASC
        """
        df = pd.read_sql(query, self.engine)
        df = self.fix_timezone(df)
        
        # Filtrer les stations qui sont vraiment dans le polygone
        if not df.empty:
            df = df[df.apply(lambda row: self.quartier_polygons[quartier].contains(Point(row['lat'], row['lon'])), axis=1)]
            df['quartier'] = quartier
            df = df.drop(['lat', 'lon'], axis=1)
        
        return df

    def run(self):
        st.set_page_config(page_title="Vélos Rennes - Live", layout="wide")
        
        if 'selected_station' not in st.session_state:
            st.session_state.selected_station = None

        st.title(" Suivi des Vélos en Libre-Service - Rennes")

        df_latest = self.load_latest_data()
        
        if df_latest.empty:
            st.error("Impossible de charger les données de la carte.")
            return

        # Récupérer la liste des quartiers
        quartiers = sorted(df_latest['quartier'].unique())
        selected_quartier = st.selectbox("Sélectionner un quartier :", ["Tous"] + list(quartiers))

        # Filtrer les données par quartier
        if selected_quartier == "Tous":
            df_filtered = df_latest
        else:
            df_filtered = df_latest[df_latest['quartier'] == selected_quartier]

        fig_map = px.scatter_mapbox(
            df_filtered, lat="lat", lon="lon", color="fill_rate", size="capacity",
            color_continuous_scale="RdYlGn", zoom=13, 
            center={"lat": df_latest['lat'].mean(), "lon": df_latest['lon'].mean()},
            mapbox_style="carto-positron",
            custom_data=['name', 'capacity', 'num_bikes_available', 'num_docks_available', 'fill_rate', 'quartier']
        )

        fig_map.update_traces(
            hovertemplate="<b>%{customdata[0]}</b><br>" +
                          "Quartier : %{customdata[5]}<br>" +
                          "capacité total de %{customdata[1]} <br> " +
                          "%{customdata[2]} velos disponible <br> " +
                          "%{customdata[3]} places libre <br>" +
                          "%{customdata[4]}% de remplissage" +
                          "<extra></extra>"
        )
        fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=450)

        map_event = st.plotly_chart(fig_map, use_container_width=True, on_select="rerun", selection_mode="points")

        if map_event and len(map_event["selection"]["points"]) > 0:
            idx = map_event["selection"]["points"][0]["point_index"]
            st.session_state.selected_station = df_filtered.iloc[idx]['name']

        # Priorité : si une station est cliquée, afficher ses infos
        if st.session_state.selected_station:
            st.divider()
            
            head1, head2 = st.columns([5, 1])
            
            # Récupérer le quartier de la station
            station_info = df_latest[df_latest['name'] == st.session_state.selected_station].iloc[0]
            quartier_station = station_info['quartier']
            
            head1.subheader(f" Analyse Station : {st.session_state.selected_station} ({quartier_station})")
            if head2.button(" Fermer"):
                st.session_state.selected_station = None
                st.rerun()

            # FILTRES
            f1, f2 = st.columns(2)
            with f1:
                default_date = df_latest['collected_at'].max().date()
                chosen_date = st.date_input("Sélectionner un jour :", default_date)
            with f2:
                time_range = st.slider("Plage horaire :", value=(time(6, 0), time(22, 0)), format="HH:mm")

            df_week = self.load_weekly_data(st.session_state.selected_station, chosen_date)

            if not df_week.empty:
                start_t, end_t = time_range
                df_day = df_week[df_week['collected_at'].dt.date == chosen_date]
                df_day_filtered = df_day[(df_day['collected_at'].dt.time >= start_t) & (df_day['collected_at'].dt.time <= end_t)]
                
                df_week_filtered = df_week[(df_week['collected_at'].dt.time >= start_t) & (df_week['collected_at'].dt.time <= end_t)]

                if not df_day_filtered.empty:
                    fig_line = px.line(
                        df_day_filtered, x="collected_at", y="num_bikes_available", markers=True,
                        title=f"Vélos disponibles le {chosen_date.strftime('%d/%m/%Y')}",
                        labels={"collected_at": "Heure", "num_bikes_available": "Vélos"}
                    )
                    fig_line.update_yaxes(rangemode="tozero")
                    st.plotly_chart(fig_line, use_container_width=True)

                    st.markdown("Statistiques suplementaires ")
                    
                    k1, k2, k3 = st.columns(3)
                    k4, k5 = st.columns(3)
                    
                    with k1:
                        st.markdown(" Min / Max** *(24h vs 7j)")
                        st.write(f"Jour : {df_day_filtered['num_bikes_available'].min()} / {df_day_filtered['num_bikes_available'].max()}")
                        st.write(f"Semaine : {df_week_filtered['num_bikes_available'].min()} / {df_week_filtered['num_bikes_available'].max()}")

                    with k2:
                        st.markdown(" Médiane Vélos")
                        st.metric("Aujourd'hui", f"{df_day_filtered['num_bikes_available'].median():.0f}")
                        st.caption(f"Moyenne Hebdo : {df_week_filtered['num_bikes_available'].median():.0f}")

                    with k3:
                        st.markdown("Remplissage Moyen")
                        st.metric("Aujourd'hui", f"{df_day_filtered['fill_rate'].mean():.1f}%")
                        st.caption(f"Moyenne Hebdo : {df_week_filtered['fill_rate'].mean():.1f}%")

                    with k4:
                        fiabilite = (df_day_filtered['num_bikes_available'] >= 2).mean() * 100
                        st.markdown("**🛡️ Fiabilité Station**")
                        st.metric("Disponibilité > 1", f"{fiabilite:.1f}%")
                        st.caption("Probabilité de trouver un vélo")


                    with k5:
                        if not df_week_filtered.empty:
                            idx_max = df_week_filtered['num_bikes_available'].idxmax()
                            heure_pic = df_week_filtered.loc[idx_max, 'collected_at'].strftime("%H:%M")
                            st.markdown("**🔥 Pic Hebdomadaire**")
                            st.metric("Heure optimale", heure_pic)
                            st.caption("Moment où la station est la plus pleine")

                else:
                    st.warning("Aucune donnée pour cette plage horaire précise.")
        
        # Sinon, si un quartier est sélectionné (pas "Tous"), afficher les infos du quartier
        elif selected_quartier != "Tous":
            st.divider()
            
            head1, head2 = st.columns([5, 1])
            head1.subheader(f" Analyse Quartier : {selected_quartier}")
            if head2.button(" Fermer le quartier"):
                st.rerun()

            # FILTRES
            f1, f2 = st.columns(2)
            with f1:
                default_date = df_latest['collected_at'].max().date()
                chosen_date = st.date_input("Sélectionner un jour :", default_date)
            with f2:
                time_range = st.slider("Plage horaire :", value=(time(6, 0), time(22, 0)), format="HH:mm")

            df_week = self.load_quartier_data(selected_quartier, chosen_date)

            if not df_week.empty:
                start_t, end_t = time_range
                df_day = df_week[df_week['collected_at'].dt.date == chosen_date]
                df_day_filtered = df_day[(df_day['collected_at'].dt.time >= start_t) & (df_day['collected_at'].dt.time <= end_t)]
                
                df_week_filtered = df_week[(df_week['collected_at'].dt.time >= start_t) & (df_week['collected_at'].dt.time <= end_t)]

                if not df_day_filtered.empty:
                    # Agréger les données du quartier
                    df_day_agg = df_day_filtered.groupby('collected_at').agg({
                        'num_bikes_available': 'sum',
                        'fill_rate': 'mean'
                    }).reset_index()
                    
                    fig_line = px.line(
                        df_day_agg, x="collected_at", y="num_bikes_available", markers=True,
                        title=f"Évolution des vélos disponibles - {selected_quartier}",
                        labels={"collected_at": "Heure", "num_bikes_available": "Vélos"}
                    )
                    fig_line.update_yaxes(rangemode="tozero")
                    st.plotly_chart(fig_line, use_container_width=True)

                    st.markdown("Statistiques suplementaires ")
                    
                    k1, k2, k3 = st.columns(3)
                    k4, k5, k6 = st.columns(3)
                    
                    with k1:
                        st.markdown(" Min / Max** *(24h vs 7j)")
                        st.write(f"Jour : {df_day_filtered['num_bikes_available'].min()} / {df_day_filtered['num_bikes_available'].max()}")
                        st.write(f"Semaine : {df_week_filtered['num_bikes_available'].min()} / {df_week_filtered['num_bikes_available'].max()}")

                    with k2:
                        st.markdown(" Médiane Vélos")
                        st.metric("Aujourd'hui", f"{df_day_filtered['num_bikes_available'].median():.0f}")
                        st.caption(f"Moyenne Hebdo : {df_week_filtered['num_bikes_available'].median():.0f}")

                    with k3:
                        st.markdown("Remplissage Moyen")
                        st.metric("Aujourd'hui", f"{df_day_filtered['fill_rate'].mean():.1f}%")
                        st.caption(f"Moyenne Hebdo : {df_week_filtered['fill_rate'].mean():.1f}%")

                    with k4:
                        fiabilite = (df_day_filtered['num_bikes_available'] >= 5).mean() * 100
                        st.markdown("**🛡️ Fiabilité Quartier**")
                        st.metric("Disponibilité > 4", f"{fiabilite:.1f}%")
                        st.caption("Probabilité de trouver des vélos")

                    with k5:
                        volatilité = df_day_filtered['num_bikes_available'].std()
                        st.markdown("**⚡ Activité (Tension)**")
                        status = "Élevée" if volatilité > 10 else "Calme"
                        st.metric("Flux de vélos", status)
                        st.caption(f"Écart-type : {volatilité:.1f}")

                    with k6:
                        if not df_week_filtered.empty:
                            idx_max = df_week_filtered['num_bikes_available'].idxmax()
                            heure_pic = df_week_filtered.loc[idx_max, 'collected_at'].strftime("%H:%M")
                            st.markdown("**🔥 Pic Hebdomadaire**")
                            st.metric("Heure optimale", heure_pic)
                            st.caption("Moment où le quartier a le plus de vélos")

                else:
                    st.warning("Aucune donnée pour cette plage horaire précise.")
            
if __name__ == "__main__":
    app = VeloDashboard()
    app.run()