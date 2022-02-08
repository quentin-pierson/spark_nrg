import pyproj
import streamlit as st
import pandas as pd
import numpy as np
import json
from IPython.display import display
import geopandas as gpd
import matplotlib.pyplot as plt

st.set_option('deprecation.showPyplotGlobalUse', False)

f = open("data/raw-file/decoupage_regions_metropole.json")

study_area = json.loads(f.read())
gdf = gpd.GeoDataFrame.from_features(study_area["features"])

gdf.crs = gdf.to_crs(pyproj.CRS.from_epsg(4326), inplace=True)
Lat = gdf['Lat']
Long = gdf['Long']

print(gdf.head())

st.map(gdf)