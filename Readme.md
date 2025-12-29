# Halifax Transit Analytics Platform

> End-to-end transit network analysis system with PostgreSQL + PostGIS, Python analytics, and interactive Streamlit dashboard

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)
![PostGIS](https://img.shields.io/badge/PostGIS-3.4-green.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.29-red.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## Table of Contents

- [Project Overview](#project-overview)
- [Key Insights](#key-insights)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Methodology](#methodology)
- [Technical Challenges](#technical-challenges)
- [Dashboard Features](#dashboard-features)
- [Future Enhancements](#future-enhancements)

---

## Project Overview

A comprehensive transit analytics platform analyzing **663,788 data points** from Halifax Transit's GTFS feed. The system provides insights into route performance, stop connectivity, service patterns, and geographic coverage through an interactive web dashboard.

### Key Features

- **PostgreSQL Database** with PostGIS spatial extension
- **ETL Pipeline** with automated data validation
- **4 SQL Analytical Views** for instant insights
- **Interactive Streamlit Dashboard** with dynamic filtering
- **Spatial Analysis** using PostGIS geographic queries
- **Temporal Pattern Analysis** revealing peak service hours

---

## Key Insights

### Network Statistics
- **80 routes** serving **2,380 bus stops**
- **13,666 scheduled trips** with **481,610 stop-time entries**
- **Route 1** (Spring Garden) runs **180 trips per service pattern** - highest frequency
- **Barrington St** hub serves **27 routes** - critical transfer point

### Connectivity Analysis
- **41% of stops** served by only 1 route (potential isolation concern)
- **152 transit hubs** with 5+ routes (6% of network)
- Average connectivity: **2.17 routes per stop**
- **973 isolated stops** located more than 500 meters from nearest neighbor

### Temporal Patterns
- **Peak hour**: 5:00 PM with **33,635 departures**
- **Morning rush** (7-9 AM): 88,975 total departures
- **Evening rush** (4-6 PM): 98,148 total departures
- **Weekend service reduction**: 47% less service on Sundays compared to weekdays

### Geographic Coverage
- Downtown Halifax contains **662 clustered stops** (highest service density)
- Service gaps identified in suburban areas
- Spatial analysis reveals coverage optimization opportunities

---

## Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     Data Sources                             │
│              Halifax Transit GTFS Feed                       │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   ETL Pipeline                               │
│  • Data validation & cleaning                                │
│  • Foreign key integrity checks                              │
│  • Date/time normalization                                   │
│  • ID type conversion                                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│            PostgreSQL + PostGIS Database                     │
│  • 11 normalized tables                                      │
│  • Spatial indexes (GIST)                                    │
│  • Foreign key constraints                                   │
│  • Automatic geometry triggers                               │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 Analytics Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ SQL Views    │  │ Python       │  │ PostGIS      │      │
│  │ • Route      │  │ Scripts      │  │ Spatial      │      │
│  │   Summary    │  │ • Performance│  │ Queries      │      │
│  │ • Stop       │  │ • Coverage   │  │ • Distance   │      │
│  │   Connect.   │  │ • Temporal   │  │ • Coverage   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│            Streamlit Dashboard                               │
│  • Interactive filtering & sorting                           │
│  • Dynamic visualizations                                    │
│  • Real-time metric updates                                  │
│  • Geographic maps with Folium                               │
└─────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

### Database
- **PostgreSQL 16** - Relational database management system
- **PostGIS 3.4** - Spatial extension for geographic queries and analysis

### Backend & Analysis
- **Python 3.11+** - Core programming language
- **pandas** - Data manipulation and analysis
- **SQLAlchemy** - SQL toolkit and Object-Relational Mapping
- **GeoPandas** - Spatial data operations
- **psycopg2** - PostgreSQL database adapter

### Visualization & Frontend
- **Streamlit** - Interactive web application framework
- **Plotly** - Interactive charts and graphs
- **Folium** - Geographic map visualizations
- **matplotlib** - Static data visualizations
- **seaborn** - Statistical data visualization

### Development Tools
- **Git/GitHub** - Version control and collaboration
- **VS Code** - Integrated development environment
- **pgAdmin** - PostgreSQL database management interface

---

## Project Structure
```
halifax-transit-analytics/
├── sql/
│   ├── schema.sql              # Database schema with PostGIS extensions
│   ├── feature_queries.sql     # 4 analytical views for metrics
│   └── indexes.sql             # Performance optimization indexes
├── src/
│   ├── etl/
│   │   └── load_gtfs.py       # ETL pipeline with validation logic
│   └── analysis/
│       ├── route_analysis.py   # Route performance metrics
│       ├── spatial_analysis.py # Geographic coverage analysis
│       └── temporal_analysis.py # Time-based service patterns
├── streamlit_app/
│   ├── Home.py                 # Dashboard home page
│   ├── .streamlit/
│   │   └── config.toml        # Streamlit configuration
│   └── pages/
│       ├── 1_Route_Analysis.py      # Interactive route filtering
│       ├── 2_Stop_Analysis.py       # Map & connectivity analysis
│       ├── 3_Temporal_Analysis.py   # Time pattern analysis
│       └── 4_About.py               # Project documentation
├── config/
│   └── database.yml            # Database credentials (not in repo)
├── outputs/
│   └── analysis/               # Generated visualizations
├── data/
│   └── raw/
│       └── gtfs_static/        # GTFS feed files (not in repo)
├── .gitignore                  # Git ignore rules
├── requirements.txt            # Python dependencies
├── LICENSE                     # MIT License
└── README.md                   # This file
```

---

## Getting Started

### Prerequisites
- Python 3.11 or higher
- PostgreSQL 16+ with PostGIS extension
- Git for version control
- 2GB free disk space for data

### Installation

#### 1. Clone the repository
```bash
git clone https://github.com/yourusername/halifax-transit-analytics.git
cd halifax-transit-analytics
```

#### 2. Create and activate virtual environment
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

#### 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

#### 4. Set up PostgreSQL database
```bash
# Create database
createdb halifax_transit_analytics

# Enable PostGIS extension
psql -d halifax_transit_analytics -c "CREATE EXTENSION postgis;"
```

#### 5. Configure database credentials
Create `config/database.yml`:
```yaml
database:
  host: localhost
  port: 5432
  database: halifax_transit_analytics
  user: postgres
  password: your_password
```

#### 6. Download GTFS data
Download Halifax Transit GTFS feed and place in `data/raw/gtfs_static/`

#### 7. Create database schema
```bash
psql -d halifax_transit_analytics -f sql/schema.sql
```

#### 8. Load GTFS data
```bash
python src/etl/load_gtfs.py
```

#### 9. Create analytical views and indexes
```bash
psql -d halifax_transit_analytics -f sql/feature_queries.sql
psql -d halifax_transit_analytics -f sql/indexes.sql
```

#### 10. Launch Streamlit dashboard
```bash
cd streamlit_app
streamlit run Home.py
```

The dashboard will open at `http://localhost:8501`

---

## Methodology

### Data Acquisition & ETL Process

1. **Data Source**: Halifax Transit GTFS (General Transit Feed Specification) static feed
2. **Validation**: Implemented foreign key integrity checks to filter invalid references
3. **Transformation**: 
   - Converted date formats from YYYYMMDD integers to PostgreSQL DATE types
   - Standardized ID fields to consistent string types
   - Handled special characters in data fields
4. **Loading**: Bulk loaded 663,788 validated records with transaction management
5. **Quality Control**: Filtered 10 invalid trips and 65 invalid calendar dates

### Database Design Principles

- **Normalization**: Followed GTFS specification for table structure
- **Referential Integrity**: Foreign key constraints enforce data relationships
- **Spatial Indexing**: PostGIS GIST indexes for efficient geographic queries
- **Query Optimization**: B-tree indexes on frequently joined columns
- **Automatic Triggers**: PostGIS geometry columns auto-populated from lat/lon coordinates

### Analytical Approach

#### Route Analysis
- Frequency metrics calculated as trips per service pattern
- Coverage analysis determining unique stops per route
- Service pattern diversity assessment

#### Spatial Analysis
- PostGIS `ST_Distance` calculations for stop proximity analysis
- Geographic clustering using spatial queries
- Coverage gap identification using distance thresholds
- Interactive map visualization with Folium library

#### Temporal Analysis
- Hourly departure pattern extraction
- Peak hour identification and quantification
- Weekday/weekend service comparison
- Service level distribution throughout the day

#### Network Analysis
- Connectivity metrics for transfer opportunity assessment
- Hub identification based on route convergence
- Network resilience evaluation

---

## Technical Challenges Solved

### 1. Data Quality & Integrity

**Challenge**: GTFS feed contained foreign key references to non-existent records

**Solution**: 
- Implemented validation layer in ETL pipeline
- Created `load_gtfs_file_with_validation()` function
- Queries referenced tables to build valid ID sets
- Filters DataFrame before insertion
- Results: Filtered 10 invalid trips (0.07%) and 65 invalid calendar dates (76%)

### 2. Data Type Mismatches

**Challenge**: trip_id stored as VARCHAR in database but read as INTEGER from CSV files

**Impact**: Join operations failed, returning zero results despite data presence

**Solution**:
- Added explicit type conversion in ETL pipeline
- Converted all ID columns to string type using `df[col].astype(str)`
- Applied consistently across all GTFS files
- Results: Successfully loaded 481,610 stop_times records

### 3. Query Performance Optimization

**Challenge**: Stop connectivity view took 4+ seconds to execute (481,610 rows)

**Analysis**: EXPLAIN ANALYZE showed:
- Sequential scans on large tables
- External merge sort using disk (39MB temp files)
- Lack of indexes on foreign key columns

**Solution**:
- Created B-tree indexes on foreign keys (trip_id, stop_id, route_id, service_id)
- Added GIST spatial indexes on geometry columns
- Implemented query result caching with `@st.cache_data`
- Results: Query time reduced to <1 second

### 4. Special Characters in Connection Strings

**Challenge**: Database passwords containing '@' symbol broke PostgreSQL connection URLs

**Solution**:
- Implemented URL encoding using `urllib.parse.quote_plus()`
- Applied to password before building connection string
- Added fallback logic for different configuration formats
- Results: Reliable connections regardless of password complexity

### 5. Large Dataset Handling

**Challenge**: Loading 663,788 rows caused memory issues and slow performance

**Solution**:
- Implemented chunked processing with pandas
- Used SQLAlchemy connection pooling
- Set `isolation_level="AUTOCOMMIT"` to avoid transaction overhead
- Truncated tables before loading to prevent duplicates
- Results: Consistent load times around 2-3 minutes

---

## Dashboard Features

### Home Page
- Real-time network statistics (routes, stops, trips, schedule entries)
- Key connectivity metrics
- System overview with navigation guide

### Route Analysis Page

**Interactive Controls:**
- Slider: Adjust number of routes displayed (5-80)
- Dropdown: Sort by frequency, total trips, route number, or stops
- Number input: Filter by minimum trips per service
- Multi-select: Choose specific routes
- Radio buttons: Switch between bar/line/area charts
- Radio buttons: Toggle between metrics (frequency, trips, stops)

**Visualizations:**
- Dynamic route comparison charts
- Summary statistics for filtered data
- Sortable data table with all metrics

### Stop Analysis Page

**Interactive Controls:**
- Slider: Filter by minimum routes per stop
- Number input: Filter by minimum daily trips
- Slider: Limit stops shown on map (performance optimization)
- Dropdown: Select map center location
- Radio buttons: Choose color scheme (connectivity vs service volume)
- Slider: Adjust number of busiest stops displayed
- Radio buttons: Sort busiest stops by trips or routes

**Visualizations:**
- Interactive Folium map with 2,380+ stops
- Color-coded markers based on connectivity or service volume
- Busiest stops bar chart
- Stop connectivity distribution histogram

### Temporal Analysis Page

**Interactive Controls:**
- Slider: Adjust hour range to display (0-23)
- Number input: Set peak hour threshold
- Dropdown: Select chart type (line/bar/area/combined)
- Checkbox: Toggle peak hour markers
- Radio buttons: Choose day type chart (bar/pie)
- Checkbox: Show/hide percentages

**Visualizations:**
- Hourly service distribution chart
- Peak period identification
- Weekday vs weekend comparison
- Service pattern statistics

### About Page
- Comprehensive project documentation
- Technology stack details
- Methodology explanation
- Project structure overview
- Future enhancement roadmap

---

## SQL Views Created

### 1. route_summary
Provides overview statistics for each route including total trips, service patterns, and unique stops served.

### 2. route_efficiency
Calculates route frequency metrics by computing average trips per service pattern, enabling identification of high and low frequency routes.

### 3. stop_connectivity
Analyzes how many routes serve each stop, identifies transit hubs, and provides route lists for each stop location.

### 4. busiest_stops
Ranks stops by total daily service volume, helping identify high-traffic locations and capacity planning needs.

---

## Performance Optimizations

### Database Level
- **Foreign Key Indexes**: B-tree indexes on trip_id, stop_id, route_id, service_id
- **Spatial Indexes**: GIST indexes on all PostGIS geometry columns
- **Query Planning**: Analyzed execution plans to optimize JOIN operations
- **Connection Pooling**: SQLAlchemy pool management for efficient connections

### Application Level
- **Streamlit Caching**: `@st.cache_data` for query results, `@st.cache_resource` for database connections
- **Lazy Loading**: Data loaded only when page is accessed
- **Pagination**: Limited map displays to 500 stops for performance
- **Result Limiting**: Default LIMIT clauses on large result sets

---

## Data Statistics
```
Database Tables:           11
Total Records:             663,788
Routes:                    80
Stops:                     2,380
Trips:                     13,666
Stop Times:                481,610
Shape Points:              165,210
Calendar Entries:          10
Calendar Date Exceptions:  20
Agency Records:            1
Feed Info:                 1
```

---

## Code Statistics
```
Total Lines of Code:       ~2,500
Python Files:              7
SQL Files:                 3
Streamlit Pages:           5
Analytical Views:          4
Database Indexes:          9
Data Visualizations:       7+
Test Coverage:             Core ETL functions
```

---

## Future Enhancements

### Real-Time Data Integration
- Integrate GTFS-RT (Real-Time) feed for live vehicle positions
- Track actual vs scheduled performance
- Monitor service disruptions and delays
- Provide real-time arrival predictions

### Machine Learning Models
- **Delay Prediction**: Random Forest or LSTM models to predict delays based on historical patterns, weather, and traffic
- **Demand Forecasting**: Time series analysis (ARIMA/Prophet) to predict ridership
- **Anomaly Detection**: Identify unusual patterns in service delivery
- **Route Optimization**: Genetic algorithms for improved route planning

### Advanced Analytics
- **Accessibility Analysis**: Wheelchair access mapping, senior mobility assessment
- **Equity Analysis**: Service distribution across demographic areas
- **Multi-Modal Integration**: Connect with bike-share, pedestrian infrastructure
- **Comparative Analysis**: Benchmark against other Canadian transit systems

### Technical Improvements
- **RESTful API**: FastAPI endpoints for programmatic access
- **Automated Data Pipeline**: Scheduled GTFS feed updates with Airflow
- **Cloud Deployment**: AWS/GCP deployment with managed PostgreSQL
- **Mobile Responsive**: Optimize dashboard for mobile devices
- **User Authentication**: Multi-user support with personalized dashboards
- **Export Functionality**: PDF report generation, CSV data exports

### Additional Visualizations
- **3D Network Topology**: Interactive 3D route visualization
- **Heatmaps**: Service intensity by geographic area and time
- **Flow Diagrams**: Passenger flow between major hubs
- **Comparison Dashboards**: Before/after analysis for route changes

---

## Development Setup

### Running Tests
```bash
# Run ETL validation tests
python -m pytest tests/test_etl.py

# Run database connection tests
python -m pytest tests/test_database.py

# Run analysis script tests
python -m pytest tests/test_analysis.py
```

### Database Management
```bash
# Backup database
pg_dump -U postgres halifax_transit_analytics > backup.sql

# Restore database
psql -U postgres halifax_transit_analytics < backup.sql

# Reset database
psql -U postgres -c "DROP DATABASE halifax_transit_analytics;"
psql -U postgres -c "CREATE DATABASE halifax_transit_analytics;"
psql -d halifax_transit_analytics -c "CREATE EXTENSION postgis;"
```

### Code Quality
```bash
# Format code with black
black src/ streamlit_app/

# Lint with flake8
flake8 src/ streamlit_app/

# Type checking with mypy
mypy src/
```

---

## Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/NewFeature`)
3. Commit your changes (`git commit -m 'Add some Feature'`)
4. Push to the branch (`git push origin feature/NewFeature`)
5. Open a Pull Request

### Contribution Areas
- Additional GTFS feeds from other cities
- New analytical views and metrics
- Performance optimizations
- Bug fixes and documentation improvements
- Test coverage expansion

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Author

**Hetang**  
Computer Science Graduate | Dalhousie University  
Halifax, Nova Scotia, Canada

- Email: hetangpatel24@gmail.com

---

## Acknowledgments

- **Halifax Transit** for providing open GTFS data
- **PostgreSQL Community** for exceptional database software
- **PostGIS Team** for powerful spatial extensions
- **Streamlit** for intuitive web framework
- **Open Source Community** for invaluable tools and libraries

---

## Citations

- General Transit Feed Specification (GTFS) Reference: https://gtfs.org/
- PostGIS Documentation: https://postgis.net/docs/
- Halifax Transit Open Data: https://www.halifax.ca/transportation/halifax-transit/open-data

---

## Contact

For questions, suggestions, or collaboration opportunities, please open an issue on GitHub or contact via email.

---

**Project Status**: Active Development | Last Updated: December 2025
