import pandas as pd
from sqlalchemy import create_engine, text
import yaml
from pathlib import Path
from urllib.parse import quote_plus

project_root = Path(__file__).parent.parent.parent
data_dir = project_root / 'data' / 'raw' / 'gtfs_static'
config_path = project_root / "config" / "database.yml"

with open(config_path, 'r') as file:
    config = yaml.safe_load(file)
db_config = config['database']

encoded_password = quote_plus(db_config['password'])

connection_string = (
    f"postgresql://{db_config['user']}:{encoded_password}"
    f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
)
engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")

tables_to_clear = [
    'stop_times', 'calendar_dates', 'trips', 
    'shapes', 'stops', 'routes', 'calendar', 
    'feed_info', 'agency'
]

with engine.connect() as conn:
    for table in tables_to_clear:
        conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
        conn.commit()
        print(f"  ✓ Cleared {table}")

def load_gtfs_file(filename, table_name):
    file_path = data_dir / filename
    
    try:
        print(f"Loading {filename}...", end=" ")
        df = pd.read_csv(file_path)
        print(f"Read {len(df)} rows from CSV.", end=" ")
        
        # Convert date columns from YYYYMMDD integer to proper dates
        date_columns = ['start_date', 'end_date', 'date', 'feed_start_date', 'feed_end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
        
        # Get the columns that exist in the database table
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            )
            db_columns = [row[0] for row in result]
        
        # Only keep columns that exist in both the CSV and the database
        columns_to_insert = [col for col in df.columns if col in db_columns]
        df_filtered = df[columns_to_insert]
        
        # Load into PostgreSQL
        df_filtered.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        print(f"✓ Loaded {len(columns_to_insert)} columns into {table_name}.")
    
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
    
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print()

def load_gtfs_file_with_validation(filename, table_name, fk_column=None, fk_table=None, fk_ref_column=None):
    """
    Load GTFS file with optional foreign key validation
    
    Parameters:
    - fk_column: column in this table that references another table
    - fk_table: the table being referenced
    - fk_ref_column: the column in the referenced table
    """
    file_path = data_dir / filename
    
    try:
        print(f"Loading {filename}...", end=" ")
        df = pd.read_csv(file_path)
        print(f"Read {len(df)} rows from CSV.", end=" ")
        
        # Convert date columns
        date_columns = ['start_date', 'end_date', 'date', 'feed_start_date', 'feed_end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')

        id_columns = ['trip_id', 'route_id', 'stop_id', 'service_id', 'shape_id']
        for col in id_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        
        # Validate foreign keys if specified
        if fk_column and fk_table and fk_ref_column:
            with engine.connect() as conn:
                result = conn.execute(text(f"SELECT {fk_ref_column} FROM {fk_table}"))
                valid_values = set(row[0] for row in result)
            
            initial_count = len(df)
            df = df[df[fk_column].isin(valid_values)]
            filtered_count = initial_count - len(df)
            
            if filtered_count > 0:
                print(f"Filtered {filtered_count} invalid rows.", end=" ")
        
        # Get database columns
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            )
            db_columns = [row[0] for row in result]
        
        # Filter columns
        columns_to_insert = [col for col in df.columns if col in db_columns]
        df_filtered = df[columns_to_insert]
        
        # Load into PostgreSQL
        df_filtered.to_sql(
            table_name,
            engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        print(f"✓ Loaded {len(df_filtered)} rows, {len(columns_to_insert)} columns into {table_name}.")
    
    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
    
    except Exception as e:
        print(f"✗ Error: {e}")
    
    print()

load_gtfs_file('agency.txt', 'agency')
load_gtfs_file('calendar.txt', 'calendar')  # ← MUST be here (not calendar_dates!)
load_gtfs_file('routes.txt', 'routes')
load_gtfs_file('stops.txt', 'stops')
load_gtfs_file('shapes.txt', 'shapes')
load_gtfs_file_with_validation('trips.txt', 'trips', fk_column='service_id', fk_table='calendar', fk_ref_column='service_id')
load_gtfs_file_with_validation('stop_times.txt', 'stop_times', fk_column='trip_id', fk_table='trips', fk_ref_column='trip_id')
load_gtfs_file_with_validation('calendar_dates.txt', 'calendar_dates', fk_column='service_id', fk_table='calendar', fk_ref_column='service_id')
load_gtfs_file('feed_info.txt', 'feed_info')

print("=" * 50)
print("Committing data to database...")
with engine.connect() as conn:
    conn.commit()
print("✓ All data committed successfully!")
print("=" * 50)