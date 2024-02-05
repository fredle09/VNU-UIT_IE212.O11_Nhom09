from time import sleep
from config import DELAY, BATCH_FOLDER

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import folium
from folium.plugins import HeatMap
import streamlit_folium as st_folium


# Define the default name columns
LAT_COLUMN = "Lat"
LONG_COLUMN = "Long"
WEIGHT_COLUMN = "Weight"
COORDINATES_COLUMN = "Coordinates"

# Default location of New York City, ZOOM_LEVEL = 12
DEFAULT_LOCATION: list[float] = [40.8465, -73.8709]  # (lat, long)
ZOOM_LEVEL: int = 14

st.set_page_config(
    page_title="Real-Time Traffic Flow Dashboard",
    layout="wide",
    page_icon="🚘"
)
st.title("Real-Time Traffic Flow Dashboard")


def get_data(path: str, **kwargs) -> pd.DataFrame:
    df: pd.DataFrame = pd.read_csv(path, **kwargs)
    return df


def heat_map(df: pd.DataFrame) -> folium.Map:
    m: folium.Map = folium.Map(
        location=DEFAULT_LOCATION,  # (lat, long)
        zoom_start=ZOOM_LEVEL,
    )
    m.add_child(folium.LatLngPopup())
    HeatMap(
        data=df[[LONG_COLUMN, LAT_COLUMN, "Vol_heat"]],
        radius=16,
        blur=16,
        min_opacity=0.2,
        max_opacity=0.6,
        gradient={0.4: "blue", 0.65: "lime", 1: "red"},
    ).add_to(m)
    return m


placeholder = st.empty()
MAX_VALUE: int = 100


while True:
    geo_df: pd.DataFrame = get_data(f"{BATCH_FOLDER}batch.csv", header=0, parse_dates=["Timestamp"])
    history_df: pd.DataFrame = get_data(f"{BATCH_FOLDER}cache.csv", header=0, parse_dates=["Timestamp"])
    prediction_df: pd.DataFrame = get_data(f"{BATCH_FOLDER}prediction.csv", header=0, parse_dates=["Timestamp"])
    
    current_timestamp = history_df["Timestamp"].max()
    
    new_prediction_df: pd.DataFrame = pd.concat(
        [history_df[history_df["Timestamp"] == current_timestamp],
         prediction_df[prediction_df["Timestamp"] > current_timestamp]],
        ignore_index=False
    )

    
    with placeholder.container():
        st.markdown(f"## The current time: {current_timestamp}")
        fig_col_1, fig_col_2 = st.columns(2)
        with fig_col_1:
            st.markdown("### HeatMap of Traffic")
            st.write("""
                This heatmap serves as a visual representation of traffic flow, utilizing color
                gradients to convey variations in traffic intensity across different geographical
                areas or specific points of interest.""")
            geo_df["Vol_heat"] = geo_df["sum(Vol)"] / MAX_VALUE
            st_folium.folium_static(heat_map(geo_df), width=700)

        with fig_col_2:
            st.markdown("### Line plot of Traffic")
            st.write("""
                This chart displays the past average traffic flow between New York boroughs as
                solid lines, with dashed lines indicating forecasted future traffic volumes. It
                provides a snapshot of historical traffic patterns and forecasts, aiding in traffic
                management and planning.
            """)
            
            fig = go.Figure()
            
            for boro in history_df["Boro"].unique():
                boro_history_df = history_df[history_df["Boro"] == boro]
                fig.add_trace(
                    go.Scatter(
                        x=boro_history_df["Timestamp"],
                        y=boro_history_df["Vol"],
                        mode="lines",
                        name=f"History - {boro}"
                    )
                )
                
                boro_prediction_df = new_prediction_df[new_prediction_df["Boro"] == boro]
                fig.add_trace(
                    go.Scatter(
                        x=boro_prediction_df["Timestamp"],
                        y=boro_prediction_df["Vol"],
                        mode="lines",
                        name=f"Prediction - {boro}",
                        line=dict(dash='dash')
                    )
                )
                
            # Update layout
            fig.update_layout(xaxis_title='Timestamp')

            # Streamlit display
            st.plotly_chart(fig, use_container_width=True)

    sleep(DELAY)
