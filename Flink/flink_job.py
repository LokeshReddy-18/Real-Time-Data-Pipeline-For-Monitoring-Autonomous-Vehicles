from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.typeinfo import Types

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

# Define Kafka Source for Sensor Data
table_env.execute_sql("""
    CREATE TABLE kafka_sensor_source (
        vehicle_id INT,
        `timestamp` TIMESTAMP(3),  
        object_speed DOUBLE,
        object_position_distance DOUBLE,
        object_size DOUBLE,
        object_direction STRING,
        object_classification STRING,
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '15' SECOND             
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'sensor_topic',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink_consumer_sensor',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'    )
""")

# Define Kafka Source for Vehicle Data
table_env.execute_sql("""
    CREATE TABLE kafka_vehicle_source (
        vehicle_id INT,
        `timestamp` TIMESTAMP(3),
        current_speed DOUBLE,
        speed_limit_violation BOOLEAN,
        latitude DOUBLE,
        longitude DOUBLE,
        battery_level DOUBLE,
        remaining_range DOUBLE,
	start_location STRING,
	destination STRING,
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '15' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'vehicle_topic',
        'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'flink_consumer_vehicle',
        'format' = 'json',
        'scan.startup.mode' = 'earliest-offset'
    )
""")

sensor_type = Types.ROW([
        Types.INT(), 
        Types.SQL_TIMESTAMP(),
        Types.DOUBLE(),  
        Types.DOUBLE(),  
        Types.DOUBLE(),  
        Types.STRING(),  
        Types.STRING()    
    ])
# Convert the Tables to DataStreams
sensor_stream = table_env.to_append_stream(
    table_env.sql_query("SELECT * FROM kafka_sensor_source"),
    sensor_type
)

vehicle_type = Types.ROW([
        Types.INT(), 
        Types.SQL_TIMESTAMP(),  
        Types.DOUBLE(), 
        Types.BOOLEAN(), 
        Types.DOUBLE(),  
        Types.DOUBLE(),   
        Types.DOUBLE(),   
        Types.DOUBLE(),
	Types.STRING(),
	Types.STRING()
    ])
vehicle_stream = table_env.to_append_stream(
    table_env.sql_query("SELECT * FROM kafka_vehicle_source"),
    vehicle_type
)

#Alerts Table Sink
table_env.execute_sql("""
    CREATE TABLE postgres_alert_sink (
        vehicle_id INT,
        `timestamp` TIMESTAMP(3),
        alert_type STRING,
        alert_message STRING
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://host.docker.internal:5432/postgres',
        'table-name' = 'alerts',
        'driver' = 'org.postgresql.Driver',
        'username' = 'postgres',
        'password' = 'mysecretpassword'
    )
""")

# Speed Violation Alert
table_env.execute_sql("""
    INSERT INTO postgres_alert_sink
    SELECT 
        vehicle_id,
        `timestamp`,
        'Speed Violation' AS alert_type,
        CONCAT('Vehicle ', CAST(vehicle_id AS STRING), ' exceeded speed limit') AS alert_message
    FROM kafka_vehicle_source
    WHERE speed_limit_violation = true

    UNION ALL

    SELECT 
        vehicle_id,
        `timestamp`,
        'Low Battery' AS alert_type,
        CONCAT('Vehicle ', CAST(vehicle_id AS STRING), ' battery low: ', CAST(battery_level AS STRING), '%') AS alert_message
    FROM kafka_vehicle_source
    WHERE battery_level < 20

    UNION ALL


    SELECT 
        s.vehicle_id,
        s.`timestamp`,
        'Collision Risk' AS alert_type,
        CONCAT('Vehicle ', CAST(s.vehicle_id AS STRING), ' detected ', s.object_classification, ' at ', 
               CAST(s.object_position_distance AS STRING), 'm moving at ', CAST(s.object_speed AS STRING), ' m/s') AS alert_message
    FROM kafka_sensor_source s
    WHERE s.object_position_distance < 3.0  
    AND s.object_speed > 5;
""")

# JDBC Sink Configuration for PostgreSQL
jdbc_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
    .with_url('jdbc:postgresql://host.docker.internal:5432/postgres') \
    .with_driver_name('org.postgresql.Driver') \
    .with_user_name('postgres') \
    .with_password('mysecretpassword') \
    .build()

# Define the JdbcSink for PostgreSQL (for sensor data)
sensor_jdbc_sink = JdbcSink.sink(
    "INSERT INTO sensor_data (vehicle_id, timestamp, object_speed, object_position_distance, object_size, object_direction, object_classification) "
    "VALUES (?, ?, ?, ?, ?, ?, ?);",
    sensor_type,
    jdbc_options,
    JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1)
        .with_batch_size(1)
        .with_max_retries(5)
        .build()
)

# Define the JdbcSink for PostgreSQL (for vehicle data)
vehicle_jdbc_sink = JdbcSink.sink(
    "INSERT INTO vehicle_data (vehicle_id, timestamp, current_speed, speed_limit_violation, latitude, longitude, battery_level, remaining_range, start_location, destination) "
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
    vehicle_type,
    jdbc_options,
    JdbcExecutionOptions.builder()
        .with_batch_interval_ms(1)
        .with_batch_size(1)
        .with_max_retries(5)
        .build()
)

# Add the sinks to the DataStreams
sensor_stream.add_sink(sensor_jdbc_sink)
vehicle_stream.add_sink(vehicle_jdbc_sink)

# Execute the Flink job
env.execute("Flink Kafka to PostgreSQL with Two Sources")
