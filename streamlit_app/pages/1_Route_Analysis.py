import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path
from urllib.parse import quote_plus

st.set_page_config(page_title="Route Analysis", page_icon="🚍", layout="wide")

st.title("🚍 Route Performance Analysis")
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

@st.cache_data
def load_all_routes():
    query = """
    SELECT 
        rs.route_short_name,
        rs.route_long_name,
        rs.total_trips,
        rs.service_patterns,
        rs.unique_stops,
        COALESCE(re.avg_trips_per_service, 0) as avg_trips_per_service
    FROM route_summary rs
    LEFT JOIN route_efficiency re ON rs.route_short_name = re.route_short_name
    ORDER BY rs.route_short_name;
    """
    return pd.read_sql(query, engine)

with st.spinner('Loading route data...'):
    df_all_routes = load_all_routes()

# INTERACTIVE CONTROLS
st.sidebar.markdown("## 🎛️ Filter Controls")

# 1. Number of routes to display
num_routes = st.sidebar.slider(
    "Number of Routes to Display",
    min_value=5,
    max_value=len(df_all_routes),
    value=20,
    step=5
)

# 2. Sort by option
sort_options = {
    "Frequency (High to Low)": ("avg_trips_per_service", False),
    "Frequency (Low to High)": ("avg_trips_per_service", True),
    "Total Trips (High to Low)": ("total_trips", False),
    "Route Number": ("route_short_name", True),
    "Number of Stops": ("unique_stops", False)
}

sort_by = st.sidebar.selectbox(
    "Sort Routes By",
    options=list(sort_options.keys())
)

# 3. Filter by minimum frequency
min_frequency = st.sidebar.number_input(
    "Minimum Trips per Service",
    min_value=0,
    max_value=int(df_all_routes['avg_trips_per_service'].max()),
    value=0,
    step=10
)

# 4. Search/filter specific routes
route_search = st.sidebar.multiselect(
    "Select Specific Routes (leave empty for all)",
    options=df_all_routes['route_short_name'].tolist(),
    default=[]
)

# Apply filters
df_filtered = df_all_routes.copy()

# Filter by minimum frequency
df_filtered = df_filtered[df_filtered['avg_trips_per_service'] >= min_frequency]

# Filter by specific routes if selected
if route_search:
    df_filtered = df_filtered[df_filtered['route_short_name'].isin(route_search)]

# Sort data
sort_col, sort_asc = sort_options[sort_by]
df_filtered = df_filtered.sort_values(sort_col, ascending=sort_asc)

# Limit to selected number
df_display = df_filtered.head(num_routes)

# Display filtered count
st.info(f"📊 Showing **{len(df_display)}** routes out of **{len(df_filtered)}** matching filters (Total: {len(df_all_routes)} routes)")

st.markdown("---")

# Display data table with selection
st.markdown("## 📋 Route Details")
st.dataframe(
    df_display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "route_short_name": "Route",
        "route_long_name": "Name",
        "total_trips": st.column_config.NumberColumn("Total Trips", format="%d"),
        "avg_trips_per_service": st.column_config.NumberColumn("Avg Trips/Service", format="%.1f"),
        "unique_stops": st.column_config.NumberColumn("Stops", format="%d"),
        "service_patterns": st.column_config.NumberColumn("Service Patterns", format="%d")
    }
)

st.markdown("---")

# Interactive charts
col1, col2 = st.columns(2)

with col1:
    # Chart type selector
    chart_type = st.radio(
        "Chart Type",
        options=["Bar Chart", "Line Chart", "Area Chart"],
        horizontal=True
    )

with col2:
    # Metric selector
    metric = st.radio(
        "Metric to Display",
        options=["Avg Trips per Service", "Total Trips", "Number of Stops"],
        horizontal=True
    )

# Map metric selection to column
metric_map = {
    "Avg Trips per Service": "avg_trips_per_service",
    "Total Trips": "total_trips",
    "Number of Stops": "unique_stops"
}
selected_metric = metric_map[metric]

# Create chart based on selection
st.markdown(f"## 📈 {metric} by Route")

if chart_type == "Bar Chart":
    fig = px.bar(
        df_display,
        x='route_short_name',
        y=selected_metric,
        hover_data=['route_long_name', 'total_trips', 'unique_stops'],
        labels={
            'route_short_name': 'Route',
            selected_metric: metric
        },
        color=selected_metric,
        color_continuous_scale='Blues'
    )
elif chart_type == "Line Chart":
    fig = px.line(
        df_display,
        x='route_short_name',
        y=selected_metric,
        markers=True,
        hover_data=['route_long_name'],
        labels={
            'route_short_name': 'Route',
            selected_metric: metric
        }
    )
else:  # Area Chart
    fig = px.area(
        df_display,
        x='route_short_name',
        y=selected_metric,
        hover_data=['route_long_name'],
        labels={
            'route_short_name': 'Route',
            selected_metric: metric
        }
    )

fig.update_layout(height=500, showlegend=False)
st.plotly_chart(fig, use_container_width=True)

# Statistics
st.markdown("---")
st.markdown("## 📊 Summary Statistics (Filtered Data)")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Routes Displayed",
        len(df_display)
    )

with col2:
    st.metric(
        "Avg Frequency",
        f"{df_display['avg_trips_per_service'].mean():.1f}"
    )

with col3:
    st.metric(
        "Total Trips",
        f"{df_display['total_trips'].sum():,}"
    )

with col4:
    st.metric(
        "Total Stops Served",
        f"{df_display['unique_stops'].sum():,}"
    )