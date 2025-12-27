CREATE OR REPLACE VIEW route_summary AS
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT t.trip_id) AS total_trips,
    COUNT(DISTINCT t.service_id) AS service_patterns,
    COUNT(DISTINCT st.stop_id) AS unique_stops
FROM routes r
LEFT JOIN trips t ON r.route_id = t.route_id
LEFT JOIN stop_times st ON t.trip_id = st.trip_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name, r.route_type
ORDER BY r.route_short_name;

CREATE OR REPLACE VIEW stop_connectivity AS
SELECT
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    COUNT(DISTINCT t.route_id) AS routes_serving_stop,
    COUNT(DISTINCT st.trip_id) AS total_trips,
    STRING_AGG(DISTINCT r.route_short_name, ', ' ORDER BY r.route_short_name) AS route_list
FROM stops s
LEFT JOIN stop_times st ON s.stop_id = st.stop_id
LEFT JOIN trips t ON st.trip_id = t.trip_id
LEFT JOIN routes r ON t.route_id = r.route_id
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
ORDER BY routes_serving_stop DESC;

CREATE OR REPLACE VIEW busiest_stops AS
SELECT 
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    COUNT(st.trip_id) AS daily_trips,
    COUNT(DISTINCT t.route_id) AS num_routes
FROM stops s
LEFT JOIN stop_times st ON s.stop_id = st.stop_id
LEFT JOIN trips t ON st.trip_id = t.trip_id
GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon
HAVING COUNT(st.trip_id) > 0
ORDER BY daily_trips DESC;

CREATE OR REPLACE VIEW route_efficiency AS
SELECT 
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    COUNT(t.trip_id) AS total_trips,
    COUNT(t.trip_id) / NULLIF(COUNT(DISTINCT t.service_id), 0) AS avg_trips_per_service
FROM routes r
LEFT JOIN trips t ON r.route_id = t.route_id
GROUP BY r.route_id, r.route_short_name, r.route_long_name
HAVING COUNT(t.trip_id) > 0
ORDER BY avg_trips_per_service DESC;