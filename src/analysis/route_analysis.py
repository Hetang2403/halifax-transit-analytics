import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

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
    route_short_name,
    route_long_name,
    total_trips,
    avg_trips_per_service
FROM route_efficiency
ORDER BY avg_trips_per_service DESC
LIMIT 20;
"""

df_routes = pd.read_sql(query, engine)
print(f"Loaded {len(df_routes)} routes")
print()

# Display top 10
print("Top 10 Most Frequent Routes:")
print(df_routes[['route_short_name', 'route_long_name', 'total_trips', 'avg_trips_per_service']].head(10).to_string(index=False))
print()

plt.figure(figsize=(14, 8))
plt.barh(df_routes['route_short_name'], df_routes['avg_trips_per_service'], color='steelblue')
plt.xlabel('Average Trips per Service Pattern', fontsize=12)
plt.ylabel('Route', fontsize=12)
plt.title('Halifax Transit - Route Frequency (Top 20 Routes)', fontsize=14, fontweight='bold')
plt.gca().invert_yaxis()  # Highest at top
plt.tight_layout()

# Save the chart
output_dir = project_root / 'outputs' / 'analysis'
output_dir.mkdir(parents=True, exist_ok=True)
plt.savefig(output_dir / 'route_frequency.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved chart: {output_dir / 'route_frequency.png'}")
plt.close()
print()

query = """
SELECT 
    routes_serving_stop,
    COUNT(*) as num_stops
FROM stop_connectivity
GROUP BY routes_serving_stop
ORDER BY routes_serving_stop;
"""

df_stop_dist = pd.read_sql(query, engine)
print(f"Loaded distribution for {df_stop_dist['num_stops'].sum()} total stops")
print()

# Display distribution
print("Stop Distribution by Number of Routes:")
print(df_stop_dist.to_string(index=False))
print()

# Create visualization
plt.figure(figsize=(12, 6))
plt.bar(df_stop_dist['routes_serving_stop'], df_stop_dist['num_stops'], color='coral', edgecolor='black')
plt.xlabel('Number of Routes Serving Stop', fontsize=12)
plt.ylabel('Number of Stops', fontsize=12)
plt.title('Halifax Transit - Stop Connectivity Distribution', fontsize=14, fontweight='bold')
plt.xticks(df_stop_dist['routes_serving_stop'])
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()

# Save the chart
plt.savefig(output_dir / 'stop_distribution.png', dpi=300, bbox_inches='tight')
print(f"✓ Saved chart: {output_dir / 'stop_distribution.png'}")
plt.close()
print()
