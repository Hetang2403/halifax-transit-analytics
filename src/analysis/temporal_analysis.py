import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path
import numpy as np

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 6)

project_root = Path(__file__).parent.parent.parent
config_path = project_root / "config" / "database.yml"

with open(config_path, 'r') as file:
    config = yaml.safe_load(file)
db_config = config['database']

from urllib.parse import quote_plus
encoded_password = quote_plus(db_config['password'])

connection_string = (
    f"postgresql://{db_config['user']}:{encoded_password}"
    f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)
engine = create_engine(connection_string)

query = """
SELECT 
    EXTRACT(HOUR FROM departure_time) as hour,
    COUNT(*) as num_departures
FROM stop_times
WHERE departure_time IS NOT NULL
GROUP BY hour
ORDER BY hour;
"""

df_hourly = pd.read_sql(query, engine)
print(f"Loaded hourly distribution for {df_hourly['num_departures'].sum():,} departures")
print()

# Display distribution
print("Departures by Hour:")
print(df_hourly.to_string(index=False))
print()

# Create visualization
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Chart 1: Bar chart of hourly departures
ax1.bar(df_hourly['hour'], df_hourly['num_departures'], color='steelblue', edgecolor='black')
ax1.set_xlabel('Hour of Day (0-23)', fontsize=12)
ax1.set_ylabel('Number of Departures', fontsize=12)
ax1.set_title('Halifax Transit - Departures by Hour', fontsize=14, fontweight='bold')
ax1.set_xticks(range(0, 24))
ax1.grid(axis='y', alpha=0.3)

# Mark peak hours
peak_hour = df_hourly.loc[df_hourly['num_departures'].idxmax(), 'hour']
ax1.axvline(x=peak_hour, color='red', linestyle='--', linewidth=2, label=f'Peak Hour ({int(peak_hour)}:00)')
ax1.legend()

# Chart 2: Line chart showing service curve
ax2.plot(df_hourly['hour'], df_hourly['num_departures'], marker='o', linewidth=2, markersize=8, color='darkorange')
ax2.fill_between(df_hourly['hour'], df_hourly['num_departures'], alpha=0.3, color='orange')
ax2.set_xlabel('Hour of Day (0-23)', fontsize=12)
ax2.set_ylabel('Number of Departures', fontsize=12)
ax2.set_title('Halifax Transit - Service Level Throughout Day', fontsize=14, fontweight='bold')
ax2.set_xticks(range(0, 24))
ax2.grid(True, alpha=0.3)

plt.tight_layout()

# Save
output_dir = project_root / 'outputs' / 'analysis'
output_dir.mkdir(parents=True, exist_ok=True)
plt.savefig(output_dir / 'hourly_service.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved chart: {output_dir / 'hourly_service.png'}")
plt.close()
print()

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
GROUP BY day_type
ORDER BY 
    CASE day_type
        WHEN 'Weekday' THEN 1
        WHEN 'Saturday' THEN 2
        WHEN 'Sunday' THEN 3
    END;
"""

df_daytype = pd.read_sql(query, engine)
print("Service by Day Type:")
print(df_daytype.to_string(index=False))
print()

# Calculate percentages
total_trips = df_daytype['num_trips'].sum()
df_daytype['percentage'] = (df_daytype['num_trips'] / total_trips * 100).round(1)

# Create visualization
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

# Chart 1: Bar chart
colors = ['#3498db', '#e74c3c', '#f39c12']
ax1.bar(df_daytype['day_type'], df_daytype['num_trips'], color=colors, edgecolor='black')
ax1.set_ylabel('Number of Trips', fontsize=12)
ax1.set_title('Scheduled Trips by Day Type', fontsize=14, fontweight='bold')
ax1.grid(axis='y', alpha=0.3)

# Add value labels on bars
for i, (day, trips) in enumerate(zip(df_daytype['day_type'], df_daytype['num_trips'])):
    ax1.text(i, trips + 50, f'{trips:,}', ha='center', fontsize=11, fontweight='bold')

# Chart 2: Pie chart
ax2.pie(df_daytype['num_trips'], labels=df_daytype['day_type'], autopct='%1.1f%%',
        colors=colors, startangle=90, textprops={'fontsize': 12, 'fontweight': 'bold'})
ax2.set_title('Service Distribution by Day Type', fontsize=14, fontweight='bold')

plt.tight_layout()

# Save
plt.savefig(output_dir / 'weekday_weekend.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved chart: {output_dir / 'weekday_weekend.png'}")
plt.close()
print()

query = """
WITH route_peaks AS (
    SELECT 
        r.route_short_name,
        r.route_long_name,
        COUNT(CASE WHEN EXTRACT(HOUR FROM st.departure_time) BETWEEN 7 AND 9 
                   THEN 1 END) as morning_peak_trips,
        COUNT(CASE WHEN EXTRACT(HOUR FROM st.departure_time) BETWEEN 16 AND 18 
                   THEN 1 END) as evening_peak_trips,
        COUNT(CASE WHEN EXTRACT(HOUR FROM st.departure_time) NOT BETWEEN 7 AND 9 
                   AND EXTRACT(HOUR FROM st.departure_time) NOT BETWEEN 16 AND 18 
                   THEN 1 END) as off_peak_trips,
        COUNT(*) as total_departures
    FROM routes r
    JOIN trips t ON r.route_id = t.route_id
    JOIN stop_times st ON t.trip_id = st.trip_id
    WHERE st.stop_sequence = 1
    GROUP BY r.route_short_name, r.route_long_name
    HAVING COUNT(*) > 0
)
SELECT *
FROM route_peaks
ORDER BY (morning_peak_trips + evening_peak_trips) DESC
LIMIT 15;
"""

df_peak = pd.read_sql(query, engine)
print(f"Analyzing top 15 routes by peak service")
print()

# Calculate peak percentage
df_peak['peak_percentage'] = ((df_peak['morning_peak_trips'] + df_peak['evening_peak_trips']) / 
                               df_peak['total_departures'] * 100).round(1)

print("Routes by Peak Hour Service:")
print(df_peak[['route_short_name', 'route_long_name', 'morning_peak_trips', 
               'evening_peak_trips', 'off_peak_trips', 'peak_percentage']].to_string(index=False))
print()

# Create stacked bar chart
fig, ax = plt.subplots(figsize=(14, 8))

x = np.arange(len(df_peak))
width = 0.6

# Stacked bars
p1 = ax.bar(x, df_peak['morning_peak_trips'], width, label='Morning Peak (7-9 AM)', color='#e74c3c')
p2 = ax.bar(x, df_peak['evening_peak_trips'], width, bottom=df_peak['morning_peak_trips'], 
            label='Evening Peak (4-6 PM)', color='#3498db')
p3 = ax.bar(x, df_peak['off_peak_trips'], width, 
            bottom=df_peak['morning_peak_trips'] + df_peak['evening_peak_trips'],
            label='Off-Peak', color='#95a5a6')

ax.set_ylabel('Number of Departures', fontsize=12)
ax.set_xlabel('Route', fontsize=12)
ax.set_title('Halifax Transit - Peak vs Off-Peak Service Distribution (Top 15 Routes)', 
             fontsize=14, fontweight='bold')
ax.set_xticks(x)
ax.set_xticklabels(df_peak['route_short_name'], rotation=0)
ax.legend(loc='upper right')
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()

# Save
plt.savefig(output_dir / 'peak_hour_analysis.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved chart: {output_dir / 'peak_hour_analysis.png'}")
plt.close()
print()

print("=" * 60)
print("TEMPORAL ANALYSIS COMPLETE!")
print(f"All charts saved to: {output_dir}")
print("=" * 60)