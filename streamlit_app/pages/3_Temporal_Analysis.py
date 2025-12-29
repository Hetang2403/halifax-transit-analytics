import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import yaml
from pathlib import Path
from urllib.parse import quote_plus

st.set_page_config(page_title="Temporal Analysis", page_icon="⏰", layout="wide")

st.title("⏰ Temporal Service Pattern Analysis")
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

# Load hourly data
@st.cache_data
def load_hourly_data():
    query = """
    SELECT 
        EXTRACT(HOUR FROM departure_time) as hour,
        COUNT(*) as num_departures
    FROM stop_times
    WHERE departure_time IS NOT NULL
    GROUP BY hour
    ORDER BY hour;
    """
    return pd.read_sql(query, engine)

@st.cache_data
def load_daytype_data():
    query = """
    WITH day_classification AS (
        SELECT 
            t.trip_id,
            CASE 
                WHEN c.monday = 1 OR c.tuesday = 1 OR c.wednesday = 1 OR c.thursday = 1 OR c.friday = 1 
                THEN 'Weekday'
                WHEN c.saturday = 1 THEN 'Saturday'
                WHEN c.sunday = 1 THEN 'Sunday'
            END as day_type
        FROM calendar c
        JOIN trips t ON c.service_id = t.service_id
    )
    SELECT 
        day_type,
        COUNT(DISTINCT trip_id) as num_trips
    FROM day_classification
    WHERE day_type IS NOT NULL
    GROUP BY day_type;
    """
    return pd.read_sql(query, engine)

with st.spinner('Loading temporal data...'):
    df_hourly = load_hourly_data()
    df_daytype = load_daytype_data()

# INTERACTIVE CONTROLS IN SIDEBAR
st.sidebar.markdown("## 🎛️ Time Controls")

# 1. Time range filter
time_range = st.sidebar.slider(
    "Hour Range to Display",
    min_value=0,
    max_value=23,
    value=(0, 23),
    step=1,
    help="Filter hourly data by time range"
)

# 2. Peak hour threshold
peak_threshold = st.sidebar.number_input(
    "Peak Hour Threshold (departures)",
    min_value=10000,
    max_value=35000,
    value=30000,
    step=1000,
    help="Departures needed to classify as peak hour"
)

# 3. Chart visualization type
viz_type = st.sidebar.selectbox(
    "Hourly Chart Type",
    options=["Line Chart", "Bar Chart", "Area Chart", "Combined"]
)

# 4. Show peak markers
show_peaks = st.sidebar.checkbox("Show Peak Hour Markers", value=True)

# Filter hourly data by time range
df_hourly_filtered = df_hourly[
    (df_hourly['hour'] >= time_range[0]) & 
    (df_hourly['hour'] <= time_range[1])
].copy()

# Hourly patterns
st.markdown("## 📈 Service Distribution by Hour")

col1, col2 = st.columns([2, 1])

with col1:
    # Create chart based on selection
    if viz_type == "Line Chart":
        fig = px.line(
            df_hourly_filtered,
            x='hour',
            y='num_departures',
            title='Departures Throughout the Day',
            labels={'hour': 'Hour of Day', 'num_departures': 'Number of Departures'},
            markers=True
        )
    elif viz_type == "Bar Chart":
        fig = px.bar(
            df_hourly_filtered,
            x='hour',
            y='num_departures',
            title='Departures Throughout the Day',
            labels={'hour': 'Hour of Day', 'num_departures': 'Number of Departures'}
        )
    elif viz_type == "Area Chart":
        fig = px.area(
            df_hourly_filtered,
            x='hour',
            y='num_departures',
            title='Departures Throughout the Day',
            labels={'hour': 'Hour of Day', 'num_departures': 'Number of Departures'}
        )
    else:  # Combined
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_hourly_filtered['hour'],
            y=df_hourly_filtered['num_departures'],
            name='Departures',
            marker_color='lightblue'
        ))
        fig.add_trace(go.Scatter(
            x=df_hourly_filtered['hour'],
            y=df_hourly_filtered['num_departures'],
            mode='lines+markers',
            name='Trend',
            line=dict(color='darkblue', width=3)
        ))
        fig.update_layout(
            title='Departures Throughout the Day (Combined View)',
            xaxis_title='Hour of Day',
            yaxis_title='Number of Departures'
        )
    
    # Mark peak hours if enabled
    if show_peaks:
        peak_hours = df_hourly_filtered[df_hourly_filtered['num_departures'] >= peak_threshold]
        for _, row in peak_hours.iterrows():
            fig.add_vline(
                x=row['hour'], 
                line_dash="dash", 
                line_color="red",
                annotation_text=f"Peak: {int(row['hour'])}:00",
                annotation_position="top"
            )
    
    # Add threshold line
    fig.add_hline(
        y=peak_threshold,
        line_dash="dot",
        line_color="orange",
        annotation_text=f"Peak Threshold: {peak_threshold:,}"
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("### 🎯 Key Metrics")
    
    peak_hour = df_hourly_filtered.loc[df_hourly_filtered['num_departures'].idxmax(), 'hour']
    peak_departures = df_hourly_filtered['num_departures'].max()
    
    st.metric("Peak Hour", f"{int(peak_hour)}:00")
    st.metric("Peak Departures", f"{peak_departures:,}")
    st.metric("Avg Departures/Hour", f"{df_hourly_filtered['num_departures'].mean():,.0f}")
    st.metric("Total (Time Range)", f"{df_hourly_filtered['num_departures'].sum():,}")
    
    st.markdown("### ⏰ Service Periods")
    
    # Calculate period stats
    morning_rush = df_hourly[(df_hourly['hour'] >= 7) & (df_hourly['hour'] <= 9)]['num_departures'].sum()
    evening_rush = df_hourly[(df_hourly['hour'] >= 16) & (df_hourly['hour'] <= 18)]['num_departures'].sum()
    
    st.metric("Morning Rush (7-9 AM)", f"{morning_rush:,}")
    st.metric("Evening Rush (4-6 PM)", f"{evening_rush:,}")

st.markdown("---")

# Weekday vs Weekend
st.markdown("## 📅 Weekday vs Weekend Service")

# Interactive controls for day type visualization
col1, col2 = st.columns(2)

with col1:
    day_chart_type = st.radio(
        "Day Type Chart",
        options=["Bar Chart", "Pie Chart"],
        horizontal=True
    )

with col2:
    show_percentages = st.checkbox("Show Percentages", value=True)

col1, col2 = st.columns(2)

# Calculate percentages
total_trips = df_daytype['num_trips'].sum()
df_daytype['percentage'] = (df_daytype['num_trips'] / total_trips * 100).round(1)

with col1:
    if day_chart_type == "Bar Chart":
        fig = px.bar(
            df_daytype,
            x='day_type',
            y='num_trips',
            title='Scheduled Trips by Day Type',
            labels={'day_type': 'Day Type', 'num_trips': 'Number of Trips'},
            color='num_trips',
            color_continuous_scale='Blues',
            text='percentage' if show_percentages else None
        )
        if show_percentages:
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
        fig.update_layout(height=400, showlegend=False)
    else:
        fig = px.pie(
            df_daytype,
            values='num_trips',
            names='day_type',
            title='Service Distribution by Day Type',
            hole=0.4
        )
        fig.update_layout(height=400)
    
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Comparison metrics
    st.markdown("### 📊 Service Comparison")
    
    weekday_trips = df_daytype[df_daytype['day_type'] == 'Weekday']['num_trips'].values[0]
    saturday_trips = df_daytype[df_daytype['day_type'] == 'Saturday']['num_trips'].values[0]
    sunday_trips = df_daytype[df_daytype['day_type'] == 'Sunday']['num_trips'].values[0]
    
    sat_reduction = ((weekday_trips - saturday_trips) / weekday_trips * 100)
    sun_reduction = ((weekday_trips - sunday_trips) / weekday_trips * 100)
    
    st.metric("Weekday Service", f"{weekday_trips:,} trips")
    st.metric("Saturday Reduction", f"{sat_reduction:.1f}%", delta=f"-{saturday_trips-weekday_trips:,}", delta_color="inverse")
    st.metric("Sunday Reduction", f"{sun_reduction:.1f}%", delta=f"-{sunday_trips-weekday_trips:,}", delta_color="inverse")
    
    # Data table
    st.markdown("### 📋 Detailed Breakdown")
    st.dataframe(
        df_daytype[['day_type', 'num_trips', 'percentage']],
        hide_index=True,
        use_container_width=True,
        column_config={
            "day_type": "Day Type",
            "num_trips": st.column_config.NumberColumn("Trips", format="%d"),
            "percentage": st.column_config.NumberColumn("Percentage", format="%.1f%%")
        }
    )

st.markdown("---")

# Service insights
st.markdown("## 💡 Dynamic Insights")

col1, col2, col3 = st.columns(3)

with col1:
    peak_hours_count = len(df_hourly_filtered[df_hourly_filtered['num_departures'] >= peak_threshold])
    st.metric("Peak Hours Identified", peak_hours_count)

with col2:
    off_peak_avg = df_hourly_filtered[df_hourly_filtered['num_departures'] < peak_threshold]['num_departures'].mean()
    st.metric("Avg Off-Peak Service", f"{off_peak_avg:,.0f}")

with col3:
    weekend_total = saturday_trips + sunday_trips
    weekend_pct = (weekend_total / total_trips * 100)
    st.metric("Weekend Service %", f"{weekend_pct:.1f}%")