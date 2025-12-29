import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path
from urllib.parse import quote_plus

st.set_page_config(
    page_title="Halifax Transit Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)

# Header
st.markdown('<p class="main-header">Halifax Transit Analytics</p>', unsafe_allow_html=True)
st.markdown("### Real-time Transit Network Analysis & Visualization")
st.markdown("---")

@st.cache_resource
def get_database_connection():
    project_root = Path(__file__).parent.parent
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
def load_metrics():
    query = """
    SELECT 
        (SELECT COUNT(*) FROM routes) as total_routes,
        (SELECT COUNT(*) FROM stops) as total_stops,
        (SELECT COUNT(*) FROM trips) as total_trips,
        (SELECT COUNT(*) FROM stop_times) as total_stop_times,
        (SELECT ROUND(AVG(routes_serving_stop)::numeric, 2) FROM stop_connectivity) as avg_routes_per_stop,
        (SELECT MAX(routes_serving_stop) FROM stop_connectivity) as max_routes_at_stop;
    """
    return pd.read_sql(query, engine)

with st.spinner('Loading network statistics...'):
    metrics = load_metrics()

# Display metrics in columns
st.markdown("## Network Overview")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Routes", f"{metrics['total_routes'][0]:,}")
    
with col2:
    st.metric("Bus Stops", f"{metrics['total_stops'][0]:,}")
    
with col3:
    st.metric("Daily Trips", f"{metrics['total_trips'][0]:,}")
    
with col4:
    st.metric("Schedule Entries", f"{metrics['total_stop_times'][0]:,}")

st.markdown("---")

# Key insights
col1, col2 = st.columns(2)

with col1:
    st.markdown("## Key Insights")
    st.markdown(f"""
    - **Average connectivity:** {metrics['avg_routes_per_stop'][0]} routes per stop
    - **Major hub:** {metrics['max_routes_at_stop'][0]} routes at busiest stop
    - **Network span:** 663,788 total data points analyzed
    - **Data source:** Halifax Transit GTFS Feed
    """)

with col2:
    st.markdown("## Analysis Features")
    st.markdown("""
    - **Route Performance** - Frequency, efficiency, coverage
    - **Interactive Maps** - Explore 2,380+ bus stops
    - **Temporal Patterns** - Peak hours, weekday/weekend
    - **Spatial Analysis** - Coverage gaps, isolation
    """)

st.markdown("---")

# Navigation guide
st.markdown("## Navigation")
st.info("""
**Use the sidebar** to navigate between different analyses:
- **Route Analysis** - Explore route performance metrics
- **Stop Analysis** - Interactive maps and connectivity
- **Temporal Analysis** - Service patterns throughout the day
- **About** - Project details and methodology
""")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
Built with Python, PostgreSQL, PostGIS, and Streamlit | Data: Halifax Transit GTFS
</div>
""", unsafe_allow_html=True)
