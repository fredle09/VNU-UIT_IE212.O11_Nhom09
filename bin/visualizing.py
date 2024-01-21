import os
from typing import Union
from time import sleep

from shapely import wkt
from pyproj import Transformer

import pandas as pd

import folium
from folium.plugins import HeatMap
import streamlit as st
import streamlit_folium as st_folium

from config import *

# Define the default name columns
LAT_COLUMN = "Lat"
LONG_COLUMN = "Long"
WEIGHT_COLUMN = "Weight"
COORDINATES_COLUMN = "Coordinates"

# Define the source and target coordinate reference systems (CRS)
# Create a transformer object
SOURCE_CRS: str = "EPSG:2263"  # EPSG using in NewYorkCity
TARGET_CRS: str = "EPSG:4326"  # WGS84 (latitude, longitude)
TRANSFORMER: Transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS, always_xy=True)

# Default location of New York City, ZOOM_LEVEL = 12
# (long, lat)
DEFAULT_LOCATION: list[float] = [-73.9624, 40.6257]
ZOOM_LEVEL: int = 12


@st.cache_resource
def convert_wkt_to_coordinates(wkt_str: str) -> tuple[float, float]:
    """
    Converts a WKT string to a tuple of coordinates.

    Args:
        wkt (str): The WKT string.

    Returns:
        tuple[float, float]: The tuple of coordinates.
    """
    wkt_geo = wkt.loads(wkt_str)
    res: tuple[float, float] = TRANSFORMER.transform(wkt_geo.x, wkt_geo.y)  # long, lat
    return res


def get_datetime_from_df(df: pd.DataFrame) -> Union[set[int], None]:
    tmp_df = df.groupby(by=["Yr", "M", "D", "HH", "MM"])
    if len(tmp_df) > 1:
        print("There are more datetime in this DataFrame")
        return None

    return tmp_df.first().index[0]


@st.cache_resource
def add_coordinates_into_df(
    df: pd.DataFrame,
    src_col: Union[str, int] = "WktGeom",
    des_col: str = COORDINATES_COLUMN,
) -> pd.DataFrame:
    """
    Add coordinates into a DataFrame based on a specified source column containing Well-Known Text (WKT) geometries.

    Parameters:
        df (pd.DataFrame): The DataFrame to add coordinates to.
        src_col (str): The name of the source column containing WKT geometries. Default is "WktGeom".
        des_col (str): The name of the destination column to store the coordinates. Default is "Coordinates".

    Returns:
        pd.DataFrame: The DataFrame with added coordinates columns.
    """
    res: pd.DataFrame = df.copy()
    res[des_col] = res[src_col].apply(
        lambda wkt_geo: convert_wkt_to_coordinates(wkt_geo)
    )
    res[LONG_COLUMN] = res[des_col].apply(lambda coord: coord[0])
    res[LAT_COLUMN] = res[des_col].apply(lambda coord: coord[1])
    return res


# @st.cache_resource
def main():
    st.title("Dashboard Real-time Traffic Data")
    m: folium.Map = folium.Map(
        location=DEFAULT_LOCATION[::-1],  # (long, lat) -> (lat, long)
        zoom_start=ZOOM_LEVEL,
    )
    m.add_child(folium.LatLngPopup())

    batch_file = f"{BATCH_FOLDER}batch.csv"
    if not os.path.exists(batch_file):
        st.warning("No batch file found!")
        sleep(DELAY / 2)
        st.rerun()

    df: pd.DataFrame = pd.read_csv(batch_file, header=0)

    # Get datetime from df
    datetime: Union[set[int], None] = get_datetime_from_df(df)
    if datetime is None:
        st.warning("There are more datetime in this DataFrame")
        sleep(DELAY / 2)
        st.rerun()
    st.write("Date: {}-{}-{} {}:{}".format(*datetime))

    # Create HeatMap for Dashboard
    df_geo: pd.DataFrame = add_coordinates_into_df(df)
    df_geo = df_geo[["SegmentID", "Vol", LAT_COLUMN, LONG_COLUMN]]
    df_geo = (
        df_geo.groupby(by=["SegmentID", LAT_COLUMN, LONG_COLUMN])
        .sum(numeric_only=True)
        .reset_index()
    )

    # Handle the case when there is no any record
    # so that the sum of vol column is not exists
    if "Vol" not in df_geo.columns:
        df_geo["Vol_heat"] = None
    else:
        MAX_VALUE: int = 150
        df_geo["Vol_heat"] = df_geo["Vol"] / MAX_VALUE

    HeatMap(
        data=df_geo[[LAT_COLUMN, LONG_COLUMN, "Vol_heat"]],
        radius=16,
        blur=16,
        min_opacity=0.4,
        max_opacity=0.6,
        gradient={0.4: "blue", 0.65: "lime", 1: "red"},
    ).add_to(m)

    c11, c12 = st.columns(2)
    with c11:
        c11.header("HeatMap of Traffic")  # type: ignore
        st_folium.st_folium(
            m,
            width=1024,
            height=700,
            returned_objects=["zoom", "center"],
        )
    with c12:
        c12.header("Data of Traffic")  # type: ignore
        c12.write(df_geo)
    sleep(DELAY)
    st.rerun()


if __name__ == "__main__":
    st.set_page_config(page_title="Dashboard Real-time Traffic Data", layout="wide")
    main()
