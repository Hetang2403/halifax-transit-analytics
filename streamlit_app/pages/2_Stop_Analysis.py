import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import yaml
from pathlib import Path
from urllib.parse import quote_plus
import folium
from streamlit_folium import st_folium

st.set_page_config(page_title="Stop Analysis", page_icon="🚏", layout="wide")

st.title("🚏 Stop Connectivity & Coverage Analysis")
st.markdown("---")

# Database connection
@st.cache_resource
def get_database_connection():
    project_root = Path(__file__).parent.parent.parent
    config_path = project_root / "config" / "database.yml"
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    db_config = config['database']
    
    encoded_password = quote_plus(db_config['password'])
    connection_string = (
        f"postgresql://{db_config['user']}:{encoded_password}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return create_engine(connection_string)

engine = get_database_connection()

# Load all stop data
@st.cache_data
def load_all_stops():
    query = """
    SELECT 
        s.stop_id,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        COALESCE(sc.routes_serving_stop, 0) as routes_serving_stop,
        COALESCE(sc.total_trips, 0) as total_trips
    FROM stops s
    LEFT JOIN stop_connectivity sc ON s.stop_id = sc.stop_id
    WHERE s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL
    ORDER BY sc.routes_serving_stop DESC NULLS LAST;
    """
    return pd.read_sql(query, engine)

@st.cache_data
def load_busiest_stops():
    query = """
    SELECT 
        stop_name,
        daily_trips,
        num_routes
    FROM busiest_stops
    ORDER BY daily_trips DESC
    LIMIT 50;
    """
    return pd.read_sql(query, engine)

with st.spinner('Loading stop data...'):
    df_all_stops = load_all_stops()
    df_busiest = load_busiest_stops()

# INTERACTIVE CONTROLS IN SIDEBAR
st.sidebar.markdown("## 🎛️ Map Controls")

# 1. Filter by connectivity
min_routes = st.sidebar.slider(
    "Minimum Routes per Stop",
    min_value=0,
    max_value=int(df_all_stops['routes_serving_stop'].max()),
    value=0,
    step=1
)

# 2. Filter by daily trips
min_trips = st.sidebar.number_input(
    "Minimum Daily Trips",
    min_value=0,
    max_value=int(df_all_stops['total_trips'].max()),
    value=0,
    step=100
)

# 3. Number of stops to show on map
max_stops = st.sidebar.slider(
    "Max Stops to Display on Map",
    min_value=50,
    max_value=len(df_all_stops),
    value=500,
    step=50,
    help="Limiting stops improves map performance"
)

# 4. Map center location
map_center_options = {
    "Downtown Halifax": [44.6488, -63.5752],
    "Dartmouth": [44.6710, -63.5684],
    "Bedford": [44.7393, -63.6786],
    "Auto (All Stops)": None
}

map_center_choice = st.sidebar.selectbox(
    "Map Center",
    options=list(map_center_options.keys())
)

# 5. Color scheme
color_scheme = st.sidebar.selectbox(
    "Stop Color Scheme",
    options=["Connectivity (Routes)", "Service Volume (Trips)"]
)

# Apply filters
df_filtered = df_all_stops.copy()
df_filtered = df_filtered[df_filtered['routes_serving_stop'] >= min_routes]
df_filtered = df_filtered[df_filtered['total_trips'] >= min_trips]
df_map = df_filtered.head(max_stops)

st.info(f"📍 Showing **{len(df_map)}** stops out of **{len(df_filtered)}** matching filters (Total: {len(df_all_stops)} stops)")

st.markdown("---")

# Busiest Stops Section
st.markdown("## 🔥 Busiest Stops")

# Number of stops to show
num_busiest = st.slider("Number of stops to display", 5, 50, 15, step=5)

col1, col2 = st.columns([2, 1])

with col1:
    # Sort option
    sort_metric = st.radio(
        "Sort by",
        options=["Daily Trips", "Number of Routes"],
        horizontal=True
    )
    
    df_busiest_display = df_busiest.head(num_busiest).copy()
    
    if sort_metric == "Daily Trips":
        sort_col = 'daily_trips'
        color_col = 'daily_trips'
    else:
        sort_col = 'num_routes'
        color_col = 'num_routes'
        df_busiest_display = df_busiest_display.sort_values(sort_col, ascending=False)
    
    fig = px.bar(
        df_busiest_display,
        x=sort_col,
        y='stop_name',
        orientation='h',
        labels={sort_col: sort_metric, 'stop_name': 'Stop'},
        title=f'Top {num_busiest} Stops by {sort_metric}',
        color=color_col,
        color_continuous_scale='Reds'
    )
    fig.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("### 📊 Statistics")
    st.metric("Most Trips/Day", f"{df_busiest.iloc[0]['daily_trips']:,}")
    st.metric("At Stop", df_busiest.iloc[0]['stop_name'][:30] + "...")
    st.metric("Serving Routes", f"{df_busiest.iloc[0]['num_routes']}")
    
    st.markdown("### 🎯 Filtered Stats")
    st.metric("Avg Routes/Stop", f"{df_filtered['routes_serving_stop'].mean():.2f}")
    st.metric("Avg Trips/Day", f"{df_filtered['total_trips'].mean():.0f}")

st.markdown("---")

# Interactive Map
st.markdown("## 🗺️ Interactive Stop Map")

# Determine map center
if map_center_choice == "Auto (All Stops)":
    map_center = [df_map['stop_lat'].mean(), df_map['stop_lon'].mean()]
else:
    map_center = map_center_options[map_center_choice]

m = folium.Map(location=map_center, zoom_start=12, tiles='OpenStreetMap')

# Color function based on selection
for idx, row in df_map.iterrows():
    if color_scheme == "Connectivity (Routes)":
        value = row['routes_serving_stop']
        if value == 0:
            color = 'gray'
        elif value == 1:
            color = 'red'
        elif value <= 3:
            color = 'orange'
        elif value <= 5:
            color = 'blue'
        else:
            color = 'green'
    else:  # Service Volume
        value = row['total_trips']
        if value == 0:
            color = 'gray'
        elif value < 500:
            color = 'lightblue'
        elif value < 1000:
            color = 'blue'
        elif value < 1500:
            color = 'orange'
        else:
            color = 'red'
    
    popup_text = f"""
    <b>{row['stop_name']}</b><br>
    Routes: {int(row['routes_serving_stop'])}<br>
    Daily Trips: {int(row['total_trips'])}
    """
    
    folium.CircleMarker(
        location=[row['stop_lat'], row['stop_lon']],
        radius=5,
        popup=folium.Popup(popup_text, max_width=300),
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        tooltip=row['stop_name']
    ).add_to(m)

st_folium(m, width=None, height=500)

# Dynamic legend based on color scheme
if color_scheme == "Connectivity (Routes)":
    st.markdown("""
    **Map Legend (Connectivity):**
    - 🟢 Green: 6+ routes (Major Hub)
    - 🔵 Blue: 4-5 routes (Transfer Point)
    - 🟠 Orange: 2-3 routes (Moderate)
    - 🔴 Red: 1 route (Isolated)
    - ⚫ Gray: No service
    """)
else:
    st.markdown("""
    **Map Legend (Service Volume):**
    - 🔴 Red: 1500+ trips/day (Very High)
    - 🟠 Orange: 1000-1499 trips/day (High)
    - 🔵 Blue: 500-999 trips/day (Moderate)
    - 🔵 Light Blue: 1-499 trips/day (Low)
    - ⚫ Gray: No service
    """)

st.markdown("---")

# Connectivity distribution
st.markdown("## 📊 Stop Connectivity Distribution")

# Chart type selector
chart_type = st.radio(
    "Visualization Type",
    options=["Bar Chart", "Histogram"],
    horizontal=True
)

if chart_type == "Bar Chart":
    connectivity_dist = df_filtered['routes_serving_stop'].value_counts().sort_index()
    fig = px.bar(
        x=connectivity_dist.index,
        y=connectivity_dist.values,
        labels={'x': 'Number of Routes', 'y': 'Number of Stops'},
        title='Stop Distribution by Route Connectivity'
    )
else:
    fig = px.histogram(
        df_filtered,
        x='routes_serving_stop',
        nbins=30,
        labels={'routes_serving_stop': 'Number of Routes'},
        title='Stop Connectivity Distribution'
    )

fig.update_layout(height=400)
st.plotly_chart(fig, use_container_width=True)