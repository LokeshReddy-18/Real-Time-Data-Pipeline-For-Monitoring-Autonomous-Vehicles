-- Table for sensor data
CREATE TABLE sensor_data (
    vehicle_id INT,
    timestamp TIMESTAMP,
    object_speed DOUBLE PRECISION,
    object_position_distance DOUBLE PRECISION,
    object_size DOUBLE PRECISION,
    object_direction VARCHAR(50),
    object_classification VARCHAR(50)
);

-- Table for vehicle data
CREATE TABLE vehicle_data (
    vehicle_id INT,
    timestamp TIMESTAMP,
    current_speed DOUBLE PRECISION,
    speed_limit_violation BOOLEAN,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    battery_level DOUBLE PRECISION,
    remaining_range DOUBLE PRECISION,
    start_location TEXT,
    destination TEXT
);


-- Table for alerts
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    vehicle_id INT,
    timestamp TIMESTAMP,
    alert_type VARCHAR(50),
    alert_message TEXT
);