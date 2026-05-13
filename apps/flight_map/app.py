import os

import pandas as pd
import plotly.express as px
import pydeck as pdk
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    Format,
    StatementState,
)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title='Live Flight Tracker',
    page_icon='✈️',
    layout='wide',
)

# ── SDK client (one instance for the process lifetime) ────────────────────────

@st.cache_resource
def get_client() -> WorkspaceClient:
    """Resolves credentials across three environments:
    - Databricks Apps: platform injects SP credentials, auto-detected by SDK.
    - Streamlit Cloud / local .env: uses DATABRICKS_HOST + STREAMLIT_CLOUD_READONLY_TOKEN.
    """
    host = os.environ.get('DATABRICKS_HOST')
    token = os.environ.get('STREAMLIT_CLOUD_READONLY_TOKEN')
    if host and token:
        return WorkspaceClient(host=host, token=token)
    return WorkspaceClient()


WAREHOUSE_ID = os.environ.get('DATABRICKS_WAREHOUSE_ID', 'a25aa5dc2aa67f12')


def _query(statement: str) -> pd.DataFrame:
    w = get_client()
    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout='30s',
    )
    if response.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f'Query failed: {response.status.error}')

    if not response.result or not response.result.data_array:
        return pd.DataFrame()

    cols = [c.name for c in response.manifest.schema.columns]
    return pd.DataFrame(response.result.data_array, columns=cols)


@st.cache_data(ttl=900)   # 15 minutes — matches pipeline schedule
def load_flights() -> pd.DataFrame:
    df = _query('SELECT * FROM workspace.default.flights_map')
    for col in ('altitude_ft', 'velocity_kts', 'aircraft_count'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    for col in ('latitude', 'longitude'):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


@st.cache_data(ttl=900)
def load_top_countries() -> pd.DataFrame:
    df = _query('SELECT * FROM workspace.default.top_countries')
    if 'aircraft_count' in df.columns:
        df['aircraft_count'] = pd.to_numeric(df['aircraft_count'], errors='coerce')
    return df


# ── Load data ─────────────────────────────────────────────────────────────────

st.title('Live Global Flight Tracker')
st.caption('Real-time ADS-B data from the OpenSky Network · refreshes every 15 minutes')

try:
    with st.spinner('Loading live flight data...'):
        df = load_flights()
        df_top = load_top_countries()
except Exception as exc:
    st.error(f'Failed to load data: {exc}')
    st.info(
        'Make sure the pipeline has run at least once and the SQL warehouse is running. '
        'Check the Databricks workspace for pipeline errors.'
    )
    st.stop()

if df.empty:
    st.warning('No flight data available yet. Trigger a pipeline run to ingest the first snapshot.')
    st.stop()

# ── Sidebar filters ───────────────────────────────────────────────────────────

st.sidebar.header('Filters')

all_countries = sorted(df['origin_country'].dropna().unique())
selected_countries = st.sidebar.multiselect(
    'Country of origin',
    options=all_countries,
    default=[],
    placeholder='All countries',
)

all_types = sorted(df['emitter_category'].fillna('Unknown').unique())
selected_types = st.sidebar.multiselect(
    'Aircraft type',
    options=all_types,
    default=[],
    placeholder='All types',
) if len(all_types) > 1 else []

all_headings = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
selected_headings = st.sidebar.multiselect(
    'Heading',
    options=all_headings,
    default=[],
    placeholder='All headings',
)

vel_min = int(df['velocity_kts'].min(skipna=True) or 0)
vel_max = int(df['velocity_kts'].max(skipna=True) or 600)
vel_range = st.sidebar.slider('Velocity (kts)', vel_min, vel_max, (vel_min, vel_max))

alt_min = int(df['altitude_ft'].min(skipna=True) or 0)
alt_max = int(df['altitude_ft'].max(skipna=True) or 45000)
alt_range = st.sidebar.slider('Altitude (ft)', alt_min, alt_max, (alt_min, alt_max))

st.sidebar.divider()
if st.sidebar.button('Refresh data'):
    st.cache_data.clear()
    st.rerun()

# ── Apply filters ─────────────────────────────────────────────────────────────

df_filtered = df.copy()
if selected_countries:
    df_filtered = df_filtered[df_filtered['origin_country'].isin(selected_countries)]
if selected_types:
    df_filtered = df_filtered[
        df_filtered['emitter_category'].fillna('Unknown').isin(selected_types)
    ]
if selected_headings:
    df_filtered = df_filtered[df_filtered['heading'].isin(selected_headings)]
df_filtered = df_filtered[
    df_filtered['velocity_kts'].fillna(0).between(*vel_range) &
    df_filtered['altitude_ft'].fillna(0).between(*alt_range)
]

# ── Hero metrics ──────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric('Airborne Aircraft', f"{len(df_filtered):,}")
c2.metric('Countries', df_filtered['origin_country'].nunique())
c3.metric('Avg Speed', f"{df_filtered['velocity_kts'].mean():.0f} kts")
c4.metric('Avg Altitude', f"{df_filtered['altitude_ft'].mean():,.0f} ft")

st.divider()

# ── Layout: map + bar chart ───────────────────────────────────────────────────

map_col, chart_col = st.columns([3, 1])

countries_sorted = sorted(df['origin_country'].dropna().unique())
PALETTE = [
    [231,76,60],[52,152,219],[46,204,113],[241,196,15],[155,89,182],
    [26,188,156],[230,126,34],[52,73,94],[22,160,133],[39,174,96],
    [41,128,185],[142,68,173],[243,156,18],[192,57,43],[23,165,137],
    [40,116,166],[125,60,152],[212,172,13],[169,50,38],[17,122,101],
]
# Single source of truth for country → color used by both the map and the bar chart
color_map_rgb = {c: PALETTE[i % len(PALETTE)] for i, c in enumerate(countries_sorted)}
color_map_rgb['United States'] = [75, 0, 130]    # dark indigo purple
color_map_rgb['Canada'] = [220, 30, 30]           # red
color_map_hex = {c: '#{:02x}{:02x}{:02x}'.format(*rgb) for c, rgb in color_map_rgb.items()}

df_filtered = df_filtered.copy()
df_filtered['color'] = df_filtered['origin_country'].map(color_map_rgb).apply(
    lambda x: x if isinstance(x, list) else [150, 150, 150]
)

with map_col:
    st.subheader('Aircraft Positions')
    view_mode = st.radio('View', ['2D Map', '3D Altitude'], index=1, horizontal=True)

    if view_mode == '3D Altitude':
        df_filtered['altitude_m'] = (df_filtered['altitude_ft'].fillna(0) * 0.3048 * 30).round(0)
        layer = pdk.Layer(
            'ScatterplotLayer',
            data=df_filtered,
            get_position='[longitude, latitude, altitude_m]',
            get_color='color',
            get_radius=12000,
            radius_min_pixels=2,
            radius_max_pixels=6,
            pickable=True,
        )
        pitch = 55
    else:
        layer = pdk.Layer(
            'ScatterplotLayer',
            data=df_filtered,
            get_position='[longitude, latitude]',
            get_color='color',
            get_radius=8000,
            radius_min_pixels=2,
            radius_max_pixels=6,
            pickable=True,
        )
        pitch = 0

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=30, longitude=0, zoom=1.5, pitch=pitch
        ),
        map_style='https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
        tooltip={
            'html': '<b>{callsign}</b><br/>{origin_country}<br/>{altitude_ft} ft · {velocity_kts} kts · {heading}',
            'style': {'backgroundColor': '#1a1a2e', 'color': 'white'},
        },
    )
    st.pydeck_chart(deck, use_container_width=True)

with chart_col:
    st.subheader('Top Countries')
    fig = px.bar(
        df_top,
        x='aircraft_count',
        y='origin_country',
        orientation='h',
        color='origin_country',
        color_discrete_map=color_map_hex,
        labels={'aircraft_count': 'Aircraft', 'origin_country': ''},
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        yaxis=dict(autorange='reversed'),
    )
    st.plotly_chart(fig, use_container_width=True)
