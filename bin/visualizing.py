from time import sleep
from config import DELAY

import numpy as np
import pandas as pd
import plotly.express as px
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
    page_title="Real-Time Dashboard",
    layout="wide",
)
st.title("Real-Time Dashboard")
st.write("This is a real-time dashboard for visualizing traffic data.")
st.write("The data is updated every 60 seconds.")
st.write("The data is stored in a CSV file.")


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
    geo_df: pd.DataFrame = get_data("batch_folder/batch.csv", header=0)
    if geo_df.empty:
        geo_df: pd.DataFrame = pd.DataFrame(
            columns=["SegmentID", "Direction", "Vol", "Timestamp", "Lat", "Long"]
        )
    else:
        geo_df.drop(columns=["Boro", "street", "fromSt", "toSt"], inplace=True)

    geo_df = (
        geo_df.groupby(by=["SegmentID", "Lat", "Long"])
        .sum(numeric_only=True)
        .reset_index()
    )

    avg_df: pd.DataFrame = get_data("batch_folder/cache.csv", header=None)
    if avg_df.empty:
        avg_df = pd.DataFrame(columns=["Boro", "Arg_Vol", "Datetime"])
    else:
        avg_df.columns = ["Boro", "Arg_Vol", "Datetime"]

    with placeholder.container():
        fig_col_1, fig_col_2 = st.columns(2)
        with fig_col_1:
            st.markdown("### HeatMap of Traffic")
            geo_df["Vol_heat"] = geo_df["Vol"] / MAX_VALUE
            st_folium.folium_static(heat_map(geo_df), width=700)

        with fig_col_2:
            st.markdown("### Line plot of Traffic")
            # visualizing_line_plot(geo_df)
            st.write("Hj")

        # st.write(geo_df)

    sleep(DELAY)
