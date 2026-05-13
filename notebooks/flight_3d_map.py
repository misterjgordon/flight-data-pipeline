# Databricks notebook source
# MAGIC %md
# MAGIC # Flight Data — 3D Global Map
# MAGIC
# MAGIC Interactive 3D visualisations powered by pydeck (deck.gl), reading from
# MAGIC the `flights_map` gold table. Two views:
# MAGIC - **Scatter** — each aircraft as a dot, colored by country of origin
# MAGIC - **Column** — 3D columns rising to each aircraft's altitude, colored by velocity

# COMMAND ----------

# MAGIC %pip install pydeck --quiet

# COMMAND ----------

import pydeck as pdk
import pandas as pd
from pyspark.sql import functions as F

df_map = spark.table('workspace.default.flights_map').toPandas()
df_airborne = df_map[df_map['altitude_ft'].notna() & (df_map['altitude_ft'] > 0)].copy()

print(f"Total aircraft:   {len(df_map):,}")
print(f"Airborne w/ alt:  {len(df_airborne):,}")
print(f"Countries:        {df_map['origin_country'].nunique()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## View 1 — Scatter Map: Aircraft Positions by Country
# MAGIC
# MAGIC Each dot is one aircraft. Color = country of origin.
# MAGIC Right-click and drag to tilt. Scroll to zoom.

# COMMAND ----------

# Assign a consistent numeric color per country for stable rendering
countries = sorted(df_map['origin_country'].dropna().unique())
PALETTE = [
    [231, 76,  60],  [52, 152, 219], [46, 204, 113], [241, 196,  15],
    [155,  89, 182], [26, 188, 156], [230, 126,  34], [236, 240, 241],
    [52,  73,  94],  [22, 160, 133], [39, 174,  96], [41, 128, 185],
    [142,  68, 173], [44,  62,  80], [243, 156,  18], [231,  76,  60],
]
country_color = {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(countries)}
df_map['color'] = df_map['origin_country'].map(country_color).apply(
    lambda x: x if isinstance(x, list) else [150, 150, 150]
)

scatter_layer = pdk.Layer(
    'ScatterplotLayer',
    data=df_map,
    get_position='[longitude, latitude]',
    get_color='color',
    get_radius=8000,
    radius_min_pixels=2,
    radius_max_pixels=8,
    pickable=True,
)

view_state = pdk.ViewState(
    latitude=30,
    longitude=0,
    zoom=1.5,
    pitch=40,
    bearing=0,
)

scatter_map = pdk.Deck(
    layers=[scatter_layer],
    initial_view_state=view_state,
    map_style='https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    tooltip={
        'html': '''
            <b>{callsign}</b><br/>
            Country: {origin_country}<br/>
            Altitude: {altitude_ft} ft<br/>
            Speed: {velocity_kts} kts<br/>
            Heading: {heading}<br/>
            Type: {emitter_category}
        ''',
        'style': {'backgroundColor': '#1a1a2e', 'color': 'white', 'fontSize': '12px'},
    },
)

scatter_map

# COMMAND ----------
# MAGIC %md
# MAGIC ## View 2 — 3D Column Map: Altitude and Velocity
# MAGIC
# MAGIC Each column rises to the aircraft's altitude. Color gradient = velocity
# MAGIC (blue = slow, yellow/red = fast). Tilt to see the Atlantic crossing corridors.

# COMMAND ----------

# Normalise velocity to a 0–255 colour range for the gradient
v_min = df_airborne['velocity_kts'].min()
v_max = df_airborne['velocity_kts'].max()
df_airborne = df_airborne.copy()
df_airborne['vel_norm'] = (
    (df_airborne['velocity_kts'] - v_min) / (v_max - v_min + 1e-9) * 255
).clip(0, 255).astype(int)
df_airborne['vel_color'] = df_airborne['vel_norm'].apply(
    lambda v: [v, int(255 - v * 0.6), int(255 - v)]
)

# Convert altitude to metres for the Z coordinate (pydeck uses metres)
df_airborne = df_airborne.copy()
df_airborne['altitude_m'] = (df_airborne['altitude_ft'] * 0.3048 * 30).round(0)

floating_layer = pdk.Layer(
    'ScatterplotLayer',
    data=df_airborne,
    # Z coordinate lifts each dot to its actual altitude
    get_position='[longitude, latitude, altitude_m]',
    get_color='vel_color',
    get_radius=12000,
    radius_min_pixels=2,
    radius_max_pixels=6,
    pickable=True,
    # Required to enable Z-axis rendering
    parameters={'depthTest': True},
)

column_view = pdk.ViewState(
    latitude=40,
    longitude=-30,
    zoom=3,
    pitch=60,
    bearing=15,
)

column_map = pdk.Deck(
    layers=[floating_layer],
    initial_view_state=column_view,
    map_style='https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    tooltip={
        'html': '''
            <b>{callsign}</b><br/>
            Country: {origin_country}<br/>
            Altitude: {altitude_ft} ft<br/>
            Speed: {velocity_kts} kts
        ''',
        'style': {'backgroundColor': '#1a1a2e', 'color': 'white'},
    },
)

column_map

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Stats

# COMMAND ----------

spark.table('workspace.default.flights_map').agg(
    F.count('*').alias('total_aircraft'),
    F.countDistinct('origin_country').alias('countries'),
    F.round(F.avg('altitude_ft'), 0).alias('avg_altitude_ft'),
    F.round(F.avg('velocity_kts'), 1).alias('avg_velocity_kts'),
    F.max('velocity_kts').alias('max_velocity_kts'),
    F.max('altitude_ft').alias('max_altitude_ft'),
).display()
