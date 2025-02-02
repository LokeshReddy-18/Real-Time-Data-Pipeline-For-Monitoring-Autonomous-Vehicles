from kafka import KafkaProducer
import json
import datetime, time
import random
import sys  


KAFKA_BROKER = 'localhost:9092'
TOPIC_SENSOR = "sensor_topic"
TOPIC_VEHICLE = "vehicle_topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)



# Define some realistic locations with names and coordinates, grouped by city
locations = {
    "Los Angeles": [
        {"name": "LAX Airport", "lat": 33.9416, "lon": -118.4085},  # Los Angeles International Airport
        {"name": "SoFi Stadium", "lat": 33.9534, "lon": -118.3383},  # SoFi Stadium in Los Angeles
        {"name": "The Grove", "lat": 34.0715, "lon": -118.3594},     # The Grove (shopping mall) in LA
        {"name": "Santa Monica Pier", "lat": 34.0100, "lon": -118.4957},  # Santa Monica Pier
        {"name": "Universal Studios", "lat": 34.1381, "lon": -118.3534},  # Universal Studios Hollywood
        {"name": "Hollywood Walk of Fame", "lat": 34.1022, "lon": -118.3263},  # Hollywood Walk of Fame
        {"name": "Ralphs Supermarket", "lat": 34.0621, "lon": -118.4476},  # Ralphs Supermarket in LA
        {"name": "Venice Beach", "lat": 33.9850, "lon": -118.4695},  # Venice Beach
        {"name": "Beverly Hills", "lat": 34.0696, "lon": -118.4053},  # Beverly Hills, Los Angeles
        {"name": "Santa Monica Mall", "lat": 34.0226, "lon": -118.4956}  # Santa Monica Mall
    ],
    "San Francisco": [
        {"name": "Golden Gate Bridge", "lat": 37.8199, "lon": -122.4783},
        {"name": "Fisherman's Wharf", "lat": 37.8080, "lon": -122.4177},
        {"name": "Alcatraz Island", "lat": 37.8267, "lon": -122.4230},
        {"name": "AT&T Park", "lat": 37.7785, "lon": -122.3893},
        {"name": "Chinatown", "lat": 37.7941, "lon": -122.4078},
        {"name": "Union Square", "lat": 37.7879, "lon": -122.4074},
        {"name": "Golden Gate Park", "lat": 37.7694, "lon": -122.4862},
    ],
    "San Diego": [
        {"name": "Balboa Park", "lat": 32.7343, "lon": -117.1441},
        {"name": "Gaslamp Quarter", "lat": 32.7115, "lon": -117.1603},
        {"name": "Old Town San Diego", "lat": 32.7542, "lon": -117.1942},
        {"name": "San Diego Zoo", "lat": 32.7353, "lon": -117.1490},
        {"name": "USS Midway Museum", "lat": 32.7137, "lon": -117.1757},
        {"name": "Coronado Island", "lat": 32.6881, "lon": -117.1735},
        {"name": "Mission Beach", "lat": 32.7743, "lon": -117.2532}
    ]
}

# Define the cities with boundaries as before
cities = {
    "Los Angeles": {
        "lat_min": 33.5, "lat_max": 34.8,
        "lon_min": -118.8, "lon_max": -117.2
    },
    "San Francisco": {
        "lat_min": 37.6, "lat_max": 37.9,
        "lon_min": -123.1, "lon_max": -122.3
    },
    "San Diego": {
        "lat_min": 32.5, "lat_max": 33.0,
        "lon_min": -117.3, "lon_max": -116.9
    }
}

# GPS Locations of roads within real-world cities or highways
gps_locations = [
    {"lat": 34.052235, "lon": -118.243683},  # Los Angeles
    {"lat": 37.774929, "lon": -122.419418},  # San Francisco
    {"lat": 32.715736, "lon": -117.161087},  # San Diego
]

# Initialize cars with random speed, battery, and location
cars = []
for i in range(1, 11):
    # Choose city randomly for car start and destination
    city_name = random.choice(list(cities.keys()))
    start_location = random.choice(locations[city_name])
    # Ensure the destination is different from the start location
    destination = random.choice([loc for loc in locations[city_name] if loc != start_location])
    
    cars.append({
        "id": i,
        "speed": random.randint(30, 80),
        "battery": 80,
        "location": random.choice(gps_locations),
        "start_location": start_location,
        "destination": destination
    })

# Helper function to simulate gradual movement in the city within the specified lat/lon boundaries
def update_location_in_city(lat, lon, speed, direction, city_name):
    city_boundaries = cities[city_name]
    delta = speed * 0.00001  # Approximate delta per second for simplicity
    
    if direction == "straight":
        lat += delta
    elif direction == "left":
        lon -= delta
    elif direction == "right":
        lon += delta
    
    # Ensure the vehicle stays within the city's boundaries
    lat = max(city_boundaries["lat_min"], min(city_boundaries["lat_max"], lat))
    lon = max(city_boundaries["lon_min"], min(city_boundaries["lon_max"], lon))
    
    return round(lat, 6), round(lon, 6)

# Function to gradually update the car's sensor and vehicle data
def generate_sensor_data(car):
    # Gradual change in object speed
    object_speed = round(car["speed"] + random.uniform(-5, 5), 2)
    object_position_distance = round(random.uniform(1, 100), 2)
    object_size = round(random.uniform(1.0, 5.0), 2)
    object_direction = random.choice(["left", "right", "straight"])
    object_classification = random.choice(["car", "pedestrian", "cyclist", "truck"])

    return {
        "vehicle_id": car["id"],
        "timestamp": datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
        "object_speed": object_speed,
        "object_position_distance": object_position_distance,
        "object_size": object_size,
        "object_direction": object_direction,
        "object_classification": object_classification
    }

# Function to simulate gradual changes in vehicle data
def generate_vehicle_data(car):
    # Gradual speed change (acceleration or deceleration), realistic range: 30-80 km/h
    car["speed"] = max(30, min(60, car["speed"] + random.uniform(-5, 5)))  # Speed limited to 30-130 km/h
    
    # Check for speed limit violation (speed > 65 km/h)
    speed_limit_violation = car["speed"] > 65

    # Battery drainage, faster drainage when speed exceeds 60 km/h
    battery_drain_rate = 0.05 if car["speed"] <= 60 else 0.1
    car["battery"] = max(0, car["battery"] - battery_drain_rate)

    # Randomly choose direction and update location
    direction = random.choice(["left", "right", "straight"])  # Random direction for simplicity
    city_name = "Los Angeles"  # Can be randomized based on car location
    car["location"]["lat"], car["location"]["lon"] = update_location_in_city(
        car["location"]["lat"], car["location"]["lon"], car["speed"], direction, city_name)

    return {
        "vehicle_id": car["id"],
        "timestamp": datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
        "current_speed": round(car["speed"], 2),
        "speed_limit_violation": speed_limit_violation,
        "latitude": car["location"]["lat"],
        "longitude": car["location"]["lon"],
        "battery_level": round(car["battery"], 2),
        "remaining_range": round(car["battery"] * 5, 2),  # Estimate remaining range based on battery
        "start_location": car["start_location"]["name"],
        "destination": car["destination"]["name"]
    }

param = sys.argv[1] if len(sys.argv) > 1 else "live"

if param.lower() == "live":
    print("Starting real-time streaming...")
    while True:
        for car in cars:
            producer.send(TOPIC_SENSOR, generate_sensor_data(car))
            producer.send(TOPIC_VEHICLE, generate_vehicle_data(car))
        time.sleep(1)
else:
    try:
        num_records = int(param)  

        print(f"Generating {num_records} records per car...")

        for _ in range(num_records):
            for car in cars:
                #print(generate_sensor_data(car))
                print(generate_vehicle_data(car))
                #producer.send(TOPIC_SENSOR, generate_sensor_data(car))
                #producer.send(TOPIC_VEHICLE, generate_vehicle_data(car))
            time.sleep(1)

        print("Data generation completed.")
    
    except ValueError:
        print("Invalid parameter! Use a number (1, 2, ...) or 'live' for continuous streaming.")