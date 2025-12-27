CREATE INDEX IF NOT EXISTS idx_stop_times_trip_id ON stop_times(trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_stop_id ON stop_times(stop_id);

CREATE INDEX IF NOT EXISTS idx_trips_route_id ON trips(route_id);
CREATE INDEX IF NOT EXISTS idx_trips_service_id ON trips(service_id);

CREATE INDEX IF NOT EXISTS idx_calendar_dates_service_id ON calendar_dates(service_id);

CREATE INDEX IF NOT EXISTS idx_shapes_shape_id ON shapes(shape_id);

CREATE INDEX IF NOT EXISTS idx_stops_geom ON stops USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_shapes_geom ON shapes USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_vehicle_positions_geom ON vehicle_positions USING GIST(geom);