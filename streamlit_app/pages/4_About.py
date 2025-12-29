import streamlit as st

st.set_page_config(page_title="About", page_icon="ℹ️", layout="wide")

st.title(" About This Project")
st.markdown("---")

# Project Overview
st.markdown("##  Project Overview")
st.markdown("""
This is an **end-to-end transit analytics platform** built to analyze Halifax Transit's public transportation network.
The project demonstrates skills in:
- **Database Design** (PostgreSQL with PostGIS)
- **ETL Development** (Python data pipelines)
- **Spatial Analysis** (Geographic queries)
- **Data Visualization** (Interactive dashboards)
- **Web Development** (Streamlit applications)
""")

st.markdown("---")

# Technical Stack
col1, col2 = st.columns(2)

with col1:
    st.markdown("##  Technology Stack")
    st.markdown("""
    **Database:**
    - PostgreSQL 16
    - PostGIS (Spatial extension)
    - 11 tables, 663,788 rows
    
    **Backend:**
    - Python 3.11+
    - pandas, SQLAlchemy
    - GeoPandas, Folium
    
    **Frontend:**
    - Streamlit
    - Plotly (Interactive charts)
    - Folium (Interactive maps)
    
    **Version Control:**
    - Git & GitHub
    - Professional commit history
    """)

with col2:
    st.markdown("##  Data Sources")
    st.markdown("""
    **Primary Data:**
    - Halifax Transit GTFS Feed
    - Static schedule data
    - 80 routes, 2,380 stops
    
    **Data Processing:**
    - Data validation & cleaning
    - Foreign key integrity checks
    - Date/time normalization
    - Spatial geometry calculation
    
    **Update Frequency:**
    - Static data (current snapshot)
    - Refreshable on demand
    """)

st.markdown("---")

# Project Structure
st.markdown("##  Project Structure")
st.code("""
halifax-transit-analytics/
├── sql/
│   ├── schema.sql              # Database schema with PostGIS
│   ├── feature_queries.sql     # Analytical views
│   └── indexes.sql             # Performance optimization
├── src/
│   ├── etl/
│   │   └── load_gtfs.py       # Data loading pipeline
│   └── analysis/
│       ├── route_analysis.py   # Route performance
│       ├── spatial_analysis.py # Geographic analysis
│       └── temporal_analysis.py # Time patterns
├── streamlit_app/
│   ├── Home.py                 # Dashboard home
│   └── pages/                  # Analysis pages
└── outputs/
    └── analysis/               # Generated visualizations
""", language="text")

st.markdown("---")

# Key Features
st.markdown("##  Key Features")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **Analytics Capabilities:**
    - Route frequency analysis
    - Stop connectivity mapping
    - Peak hour identification
    - Coverage gap detection
    - Network resilience metrics
    """)

with col2:
    st.markdown("""
    **Visualizations:**
    - Interactive maps (2,380+ stops)
    - Temporal heatmaps
    - Route comparison charts
    - Service distribution graphs
    - Geographic coverage plots
    """)

st.markdown("---")

# Methodology
st.markdown("##  Methodology")

with st.expander(" Data Acquisition & ETL"):
    st.markdown("""
    1. **Downloaded** Halifax Transit GTFS feed
    2. **Validated** data quality and integrity
    3. **Transformed** date formats and ID types
    4. **Loaded** into PostgreSQL with validation
    5. **Created** PostGIS geometries from coordinates
    """)

with st.expander(" Database Design"):
    st.markdown("""
    - **Normalized schema** following GTFS specification
    - **Foreign key constraints** for referential integrity
    - **PostGIS triggers** for automatic geometry updates
    - **B-tree indexes** on foreign keys
    - **GIST indexes** on spatial columns
    """)

with st.expander(" Analysis Approach"):
    st.markdown("""
    - **Route Analysis:** Frequency, coverage, efficiency metrics
    - **Spatial Analysis:** PostGIS distance calculations, coverage gaps
    - **Temporal Analysis:** Hourly patterns, weekday/weekend comparison
    - **Network Analysis:** Connectivity, hub identification, resilience
    """)

st.markdown("---")

# Future Enhancements
st.markdown("##  Future Enhancements")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **Planned Features:**
    - Real-time vehicle tracking (GTFS-RT)
    - Delay prediction models (ML)
    - Route optimization algorithms
    - Accessibility analysis
    """)

with col2:
    st.markdown("""
    **Technical Improvements:**
    - Automated data refresh pipeline
    - API endpoint development
    - Mobile-responsive design
    - Performance caching
    """)

st.markdown("---")

# Contact & Links
st.markdown("## Contact & Repository")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    **GitHub Repository:**
    
    [View Source Code →](https://github.com/Hetang2403/halifax-transit-analytics)
    """)


with col2:
    st.markdown("""
    **Built By:**
    
    Hetang
    
    Computer Science Graduate
    """)

with col3:
    st.markdown("""
    **Technologies:**
    
    Python • PostgreSQL • PostGIS • Streamlit
    """)

with col4:
    st.markdown("""
    **E-mail:**
    
    hetangpatel24@gmail.com
                
    **Phone:**

    +1 (902) 989-8240  
    """)

st.markdown("---")

# Footer
st.markdown("""
<div style='text-align: center; color: gray; padding: 2rem;'>
<p>Built with Python, PostgreSQL, PostGIS, and Streamlit</p>
<p>Data: Halifax Transit GTFS Feed | Last Updated: December 2025</p>
</div>
""", unsafe_allow_html=True)