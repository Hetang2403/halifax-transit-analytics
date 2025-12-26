CREATE TABLE agency (
    agency_id VARCHAR(50) PRIMARY KEY,
    agency_name VARCHAR(100),
    agency_url VARCHAR(255),
    agency_timezone VARCHAR(50),
    agency_lang VARCHAR(10)
);

CREATE TABLE routes (
    route_id VARCHAR(20) PRIMARY KEY,
    agency_id VARCHAR(50) REFERENCES agency(agency_id),
    route_short_name VARCHAR(10),
    route_long_name VARCHAR(100),
    route_type INTEGER,
    route_color VARCHAR(6),
    route_text_color VARCHAR(6)
);

CREATE TABLE calendar (
    service_id VARCHAR(50) PRIMARY KEY,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date DATE,
    end_date DATE
);

CREATE TABLE calendar_dates (
    service_id VARCHAR(50) REFERENCES calendar(service_id),
    date DATE,
    exception_type INTEGER,
    PRIMARY KEY (service_id, date)
);

CREATE TABLE stops (
    stop_id VARCHAR(20) PRIMARY KEY,
    stop_code VARCHAR(20),
    stop_name VARCHAR(100),
    stop_desc TEXT,
    stop_lat DECIMAL(10, 8),
    stop_lon DECIMAL(11, 8),
    zone_id VARCHAR(20),
    stop_url VARCHAR(255),
    location_type INTEGER,
    parent_station VARCHAR(20),
    geom GEOMETRY(Point, 4326)
);

CREATE TABLE shapes (
    shape_id VARCHAR(50),
    shape_pt_lat DECIMAL(10, 8),
    shape_pt_lon DECIMAL(11, 8),
    shape_pt_sequence INTEGER,
    shape_dist_traveled DECIMAL(10, 2),
    geom GEOMETRY(Point, 4326),
    PRIMARY KEY (shape_id, shape_pt_sequence)
);

CREATE TABLE trips (
    trip_id VARCHAR(50) PRIMARY KEY,
    route_id VARCHAR(20) REFERENCES routes(route_id),
    service_id VARCHAR(50) REFERENCES calendar(service_id),
    trip_headsign VARCHAR(100),
    trip_short_name VARCHAR(50),
    direction_id INTEGER,
    block_id VARCHAR(20),
    shape_id VARCHAR(50),
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER
);

CREATE TABLE stop_times (
    trip_id VARCHAR(50) REFERENCES trips(trip_id),
    arrival_time INTERVAL,
    departure_time INTERVAL,
    stop_id VARCHAR(20) REFERENCES stops(stop_id),
    stop_sequence INTEGER,
    stop_headsign VARCHAR(100),
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled DECIMAL(10, 2),
    timepoint INTEGER,
    PRIMARY KEY (trip_id, stop_sequence)
);

CREATE TABLE feed_info (
    feed_publisher_name VARCHAR(100),
    feed_publisher_url VARCHAR(255),
    feed_lang VARCHAR(10),
    feed_start_date DATE,
    feed_end_date DATE,
    feed_version VARCHAR(50)
);

CREATE TABLE vehicle_positions (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50),
    trip_id VARCHAR(50),
    route_id VARCHAR(20),
    position_lat DECIMAL(10, 8),
    position_lon DECIMAL(11, 8),
    bearing DECIMAL(5, 2),
    speed DECIMAL(5, 2),
    timestamp TIMESTAMP,
    geom GEOMETRY(Point, 4326)
);

CREATE TABLE trip_updates (
    id SERIAL PRIMARY KEY,
    trip_id VARCHAR(50),
    route_id VARCHAR(20),
    stop_id VARCHAR(20),
    arrival_delay INTEGER,
    departure_delay INTEGER,
    timestamp TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_stops_geom()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geom = ST_SetSRID(ST_MakePoint(NEW.stop_lon, NEW.stop_lat), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_stops_geom
BEFORE INSERT OR UPDATE ON stops
FOR EACH ROW
EXECUTE FUNCTION update_stops_geom();

-- Trigger for shapes table
CREATE OR REPLACE FUNCTION update_shapes_geom()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geom = ST_SetSRID(ST_MakePoint(NEW.shape_pt_lon, NEW.shape_pt_lat), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_shapes_geom
BEFORE INSERT OR UPDATE ON shapes
FOR EACH ROW
EXECUTE FUNCTION update_shapes_geom();

-- Trigger for vehicle_positions table
CREATE OR REPLACE FUNCTION update_vehicle_positions_geom()
RETURNS TRIGGER AS $$
BEGIN
    NEW.geom = ST_SetSRID(ST_MakePoint(NEW.position_lon, NEW.position_lat), 4326);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_vehicle_positions_geom
BEFORE INSERT OR UPDATE ON vehicle_positions
FOR EACH ROW
EXECUTE FUNCTION update_vehicle_positions_geom();