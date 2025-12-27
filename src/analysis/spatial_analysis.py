import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
from folium import plugins
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path

# Set visualization style
sns.set_style("whitegrid")

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
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    sc.routes_serving_stop,
    sc.total_trips
FROM stops s
LEFT JOIN stop_connectivity sc ON s.stop_id = sc.stop_id
WHERE s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL;
"""

df_stops = pd.read_sql(query, engine)
print(f"Loaded {len(df_stops)} stops with geographic coordinates")
print()

# Create base map centered on Halifax
halifax_center = [44.6488, -63.5752]  # Downtown Halifax
m = folium.Map(location=halifax_center, zoom_start=12, tiles='OpenStreetMap')

# Add stop markers with color based on connectivity
print("Adding stop markers to map...")

# Create marker cluster for better performance
marker_cluster = plugins.MarkerCluster().add_to(m)

for idx, row in df_stops.iterrows():
    # Color based on number of routes
    if pd.isna(row['routes_serving_stop']) or row['routes_serving_stop'] == 0:
        color = 'gray'
        icon = 'ban'
    elif row['routes_serving_stop'] == 1:
        color = 'red'
        icon = 'stop'
    elif row['routes_serving_stop'] <= 3:
        color = 'orange'
        icon = 'bus'
    elif row['routes_serving_stop'] <= 5:
        color = 'blue'
        icon = 'exchange'
    else:
        color = 'green'
        icon = 'star'
    
    # Create popup with stop info
    popup_text = f"""
    <b>{row['stop_name']}</b><br>
    Stop ID: {row['stop_id']}<br>
    Routes: {int(row['routes_serving_stop']) if pd.notna(row['routes_serving_stop']) else 0}<br>
    Daily Trips: {int(row['total_trips']) if pd.notna(row['total_trips']) else 0}
    """
    
    folium.Marker(
        location=[row['stop_lat'], row['stop_lon']],
        popup=folium.Popup(popup_text, max_width=300),
        icon=folium.Icon(color=color, icon=icon, prefix='fa'),
        tooltip=row['stop_name']
    ).add_to(marker_cluster)

# Add legend
legend_html = '''
<div style="position: fixed; 
     bottom: 50px; right: 50px; width: 200px; height: 180px; 
     background-color: white; border:2px solid grey; z-index:9999; 
     font-size:14px; padding: 10px">
     <p style="margin-bottom:5px;"><b>Stop Connectivity</b></p>
     <p style="margin:2px;"><i class="fa fa-star" style="color:green"></i> 6+ routes (Hub)</p>
     <p style="margin:2px;"><i class="fa fa-exchange" style="color:blue"></i> 4-5 routes</p>
     <p style="margin:2px;"><i class="fa fa-bus" style="color:orange"></i> 2-3 routes</p>
     <p style="margin:2px;"><i class="fa fa-stop" style="color:red"></i> 1 route</p>
     <p style="margin:2px;"><i class="fa fa-ban" style="color:gray"></i> No service</p>
</div>
'''
m.get_root().html.add_child(folium.Element(legend_html))

# Save map
output_dir = project_root / 'outputs' / 'analysis'
output_dir.mkdir(parents=True, exist_ok=True)
map_path = output_dir / 'halifax_stops_map.html'
m.save(str(map_path))

query = """
WITH stop_distances AS (
    SELECT 
        s1.stop_id,
        s1.stop_name,
        s1.stop_lat,
        s1.stop_lon,
        MIN(ST_Distance(s1.geom::geography, s2.geom::geography)) as distance_to_nearest
    FROM stops s1
    CROSS JOIN stops s2
    WHERE s1.stop_id != s2.stop_id
    GROUP BY s1.stop_id, s1.stop_name, s1.stop_lat, s1.stop_lon
)
SELECT 
    stop_name,
    stop_lat,
    stop_lon,
    ROUND(distance_to_nearest::numeric, 0) as nearest_stop_meters
FROM stop_distances
WHERE distance_to_nearest > 500
ORDER BY distance_to_nearest DESC;
"""

df_isolated = pd.read_sql(query, engine)
print(f"Found {len(df_isolated)} isolated stops (>500m from nearest stop)")
print()

if len(df_isolated) > 0:
    print("Most Isolated Stops:")
    print(df_isolated.head(10).to_string(index=False))
    print()

# Visualize isolation distribution
if len(df_isolated) > 0:
    plt.figure(figsize=(12, 6))
    plt.hist(df_isolated['nearest_stop_meters'], bins=30, color='coral', edgecolor='black')
    plt.xlabel('Distance to Nearest Stop (meters)', fontsize=12)
    plt.ylabel('Number of Stops', fontsize=12)
    plt.title('Halifax Transit - Stop Isolation Analysis', fontsize=14, fontweight='bold')
    plt.axvline(x=500, color='red', linestyle='--', linewidth=2, label='500m threshold')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    plt.savefig(output_dir / 'stop_isolation.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved chart: {output_dir / 'stop_isolation.png'}")
    plt.close()
print()

stats_query = """
SELECT 
    (SELECT COUNT(*) FROM routes) as total_routes,
    (SELECT COUNT(*) FROM stops) as total_stops,
    (SELECT COUNT(*) FROM trips) as total_trips,
    (SELECT COUNT(*) FROM stop_times) as total_stop_times,
    (SELECT ROUND(AVG(routes_serving_stop)::numeric, 2) FROM stop_connectivity) as avg_routes_per_stop,
    (SELECT MAX(routes_serving_stop) FROM stop_connectivity) as max_routes_at_stop,
    (SELECT COUNT(*) FROM stop_connectivity WHERE routes_serving_stop >= 5) as hub_stops,
    (SELECT COUNT(*) FROM stop_connectivity WHERE routes_serving_stop = 1) as isolated_stops;
"""

stats = pd.read_sql(stats_query, engine)

print("Halifax Transit Network Statistics:")
print("=" * 60)
print(f"Total Routes:              {stats['total_routes'][0]:,}")
print(f"Total Stops:               {stats['total_stops'][0]:,}")
print(f"Total Scheduled Trips:     {stats['total_trips'][0]:,}")
print(f"Total Stop-Time Entries:   {stats['total_stop_times'][0]:,}")
print()
print(f"Average Routes per Stop:   {stats['avg_routes_per_stop'][0]}")
print(f"Max Routes at Single Stop: {stats['max_routes_at_stop'][0]}")
print(f"Transit Hub Stops (5+ routes): {stats['hub_stops'][0]}")
print(f"Isolated Stops (1 route):      {stats['isolated_stops'][0]:,}")
print("=" * 60)
print()
