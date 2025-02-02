import psycopg2
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st
import time
import plotly.express as px
import pytz
import requests
from PIL import Image
from io import BytesIO
import folium
from folium.map import Icon
from streamlit.components.v1 import html
from streamlit_folium import st_folium

# Database Configuration (Update with your actual DB credentials)
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    "port": "5432"
}

# Function to fetch data from the database
def fetch_data(vehicle_id=None):
    with psycopg2.connect(**DB_CONFIG) as conn:
        query = """
            SELECT v.vehicle_id, v.timestamp AS vehicle_timestamp, v.latitude, v.longitude, v.current_speed, v.battery_level, v.start_location, v.destination,
                   s.timestamp AS sensor_timestamp, s.object_speed, s.object_position_distance, s.object_size, s.object_direction, s.object_classification
            FROM vehicle_data v
            LEFT JOIN sensor_data s ON v.vehicle_id = s.vehicle_id AND v.timestamp = s.timestamp
        """
        if vehicle_id:
            query += f" WHERE v.vehicle_id = {vehicle_id}"
        query += " ORDER BY v.timestamp DESC LIMIT 1"
        df = pd.read_sql(query, conn)
    return df

def fetch_alerts():
    with psycopg2.connect(**DB_CONFIG) as conn:
        query = """
            SELECT a.alert_type, a.alert_message
            FROM alerts a
            ORDER BY a.timestamp DESC LIMIT 3
        """
        df = pd.read_sql(query, conn)
    return df


alerts_data = fetch_alerts()

def get_weather(latitude, longitude):
    api_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        st.error("Failed to fetch weather data.")
        return None

weather_conditions = {
    0: ("Clear Sky", "https://cdn-icons-png.flaticon.com/512/869/869869.png"),
    1: ("Mainly Clear", "https://cdn-icons-png.flaticon.com/512/1163/1163661.png"),
    2: ("Partly Cloudy", "https://cdn-icons-png.flaticon.com/512/1163/1163624.png"),
    3: ("Overcast", "https://cdn-icons-png.flaticon.com/512/414/414825.png"),
    45: ("Fog", "https://cdn-icons-png.flaticon.com/512/4150/4150897.png"),
    48: ("Rime Fog", "https://cdn-icons-png.flaticon.com/512/4150/4150897.png"),
    51: ("Drizzle", "https://www.svgrepo.com/show/252490/weather-rain.svg"),
    61: ("Rain", "https://www.freeiconspng.com/uploads/cloud-rain-icon-2.png"),
    71: ("Snow", "https://cdn-icons-png.flaticon.com/512/642/642102.png"),
    95: ("Thunderstorm", "https://cdn-icons-png.flaticon.com/512/1146/1146869.png")
}


# Streamlit app configuration
st.set_page_config(layout="wide")
col5, col6 = st.columns([3, 1])

with col5:
    st.title("Real-Time Vehicle Monitoring Dashboard")

with col6:
    vehicle_filter = st.selectbox("Select Vehicle", options=["All"] + [str(i) for i in range(1, 11)])

# Fetch data
data = fetch_data(vehicle_id=None if vehicle_filter == "All" else vehicle_filter)

# Layout adjustments
col1, col2, col3 = st.columns([1,1,1.8], gap="small")

# Speed Gauge
with col1:

    if not data.empty:
        speed = data['current_speed'].iloc[0]  # Current speed from vehicle_data
        speed_gauge = go.Figure(go.Indicator(
        domain = {'x': [0, 1], 'y': [0, 1]},
        value = speed,
        mode = "gauge+number",
        title = {'text': "Speed"},
        gauge = {'axis': {'range': [None, 150]},
                'steps' : [
                    {'range': [0, 40], 'color': "gray"},
                    {'range': [41, 80], 'color': "gray"}],
                'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 100}}))
        st.plotly_chart(speed_gauge, use_container_width=True)
    else:
        st.write("No speed data available.")

    if not data.empty:
        st.header("Trip Details")
        start_location = data['start_location'].iloc[0]
        destination = data['destination'].iloc[0]

        start_icon = "📍"       # Pin icon for start
        destination_icon = "🎯"  # Target icon for destination

        # Box for Start Location with neutral styling
        st.markdown(f"""
        <div style="background-color:#F5F5F5; color:#333333; padding:15px; margin:10px 0; border-radius:8px; display:flex; align-items:center; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);">
            <div style="font-size:24px; margin-right:15px;">{start_icon}</div>
            <div>
                <strong>Start Location:</strong> {start_location}
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Box for Destination Location with neutral styling
        st.markdown(f"""
        <div style="background-color:#F5F5F5; color:#333333; padding:15px; margin:10px 0; border-radius:8px; display:flex; align-items:center; box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);">
            <div style="font-size:24px; margin-right:15px;">{destination_icon}</div>
            <div>
                <strong>Destination:</strong> {destination}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.write("No trip details available")


with col2:
    if not data.empty:
        # Extract the latest timestamp
        latest_timestamp = data['vehicle_timestamp'].iloc[0]
        latest_timestamp = pd.to_datetime(latest_timestamp)

        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.tz_localize('UTC')

        # Convert to California Time (PST/PDT)
        california_tz = pytz.timezone('US/Pacific')
        latest_timestamp_california = latest_timestamp.astimezone(california_tz)

        # Format Date and Time
        formatted_date = latest_timestamp_california.strftime("%b %d, %Y")
        formatted_time = latest_timestamp_california.strftime("%I:%M %p %Z")

        # Stylish Date and Time Card
        st.markdown(f"""
            <div style="
                background-color: #F5F5F5;
                padding: 20px;
                border-radius: 12px;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                text-align: center;
                width: 300px;
                margin: 50px auto 0 auto;
                font-family: Arial, sans-serif;
            ">
                <h2 style="color: #333; margin-bottom: 5px;">🗓️ {formatted_date}</h2>
                <h3 style="color: #555; margin-top: 0;">⏰ {formatted_time}</h3>
            </div>
        """, unsafe_allow_html=True)
    else:
        st.write("No data available.")


    if not data.empty:
        battery_level = data['battery_level'].iloc[0] 

        if battery_level > 75:
            color = "#4CAF50"  # Green
        elif 50 < battery_level <= 75:
            color = "#FFEB3B"  # Yellow
        elif 25 < battery_level <= 50:
            color = "#FF9800"  # Orange
        else:
            color = "#F44336"  # Red

        # Display battery bar with percentage
        st.markdown(f"""
        <div style="display: flex; justify-content: center; align-items: center; height: 150px;">
            <div style="background-color: #E0E0E0; padding: 10px; border-radius: 10px; width: 300px; position: relative;">
                <div style="width: {battery_level}%; background-color: {color}; height: 30px; border-radius: 8px;">
                </div>
                <div style="position: absolute; top: 5px; left: 50%; transform: translateX(-50%); font-size: 18px; font-weight: bold; color: black;">
                    {battery_level:.2f}%
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.write("No battery data available.")

    if not data.empty:
    # Extract latitude and longitude from the data
        latitude = float(data['latitude'].iloc[0])
        longitude = float(data['longitude'].iloc[0])

        # Fetch weather data
        current_weather = get_weather(latitude, longitude)

        if current_weather:
            try:
                weather_code = current_weather['current_weather']['weathercode']
                temperature_celsius = current_weather['current_weather']['temperature']
                
                # Convert temperature from Celsius to Fahrenheit
                temperature = int((temperature_celsius * 9/5) + 32)
                
                # Get weather description and icon URL
                description, icon_url = weather_conditions.get(weather_code, ("Unknown", "https://cdn-icons-png.flaticon.com/512/414/414825.png"))
                
                # Load weather icon
                response = requests.get(icon_url)
                img = Image.open(BytesIO(response.content))

                # Center the image and text using HTML in Markdown
                st.markdown(f'<div style="text-align: center;">'
                            f'<img src="{icon_url}" width="200" style="display: block; margin: 0 auto;" />'
                            f'<h3>{description}</h3>'
                            f'<p><strong>{temperature}°F</strong></p>'
                            f'</div>', unsafe_allow_html=True)

            except KeyError as e:
                st.error(f"Error fetching data for weather code {weather_code}: {e}")
        else:
            st.error("Failed to fetch weather data.")
    else:
        st.error("No data available to fetch weather information.")


car_icon_url = "/Users/lokesh/Desktop/Data_Engineering/Projects/av_pipeline/car.webp"
data['icon'] = car_icon_url
# Sensor Data Section
with col3:
    if not alerts_data.empty:
        # Extract the alerts
        latest_alerts = alerts_data[['alert_type', 'alert_message']].dropna().tail(3)  # Get the last 3 alerts
        
        st.header("Latest Alerts")
        
        # Display the alerts with a custom style
        for i, row in latest_alerts.iterrows():
            alert_type = row['alert_type']
            alert_message = row['alert_message']
            
            # Define the alert icon and color based on alert type
            if alert_type == "Collision Risk":
                icon = "⚠️"
                color = "#A9A9A9"  # Yellow
            elif alert_type == "Speed Violation":
                icon = "🚨"
                color = "#A9A9A9"  # Red
            else:
                icon = "ℹ️"
                color = "#A9A9A9"  # Blue
            st.markdown(f"""
            <div style="background-color:{color}; color:white; padding:10px; margin:10px 0; border-radius:10px; display:flex; align-items:center;">
                <div style="font-size:24px; margin-right:10px;">{icon}</div>
                <div>
                    <strong>{alert_type}:</strong> {alert_message}
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.write("No alert data available.")

    # Map with car location
    if not data.empty:
        lat = data['latitude'].iloc[0] 
        long = data['longitude'].iloc[0]
        car_location = [lat,long]
        m = folium.Map(location=car_location, zoom_start=14, height=500, tiles="Esri WorldImagery")

        # Create a custom car icon (you can use Font Awesome icons or upload a custom image)
        car_icon = Icon(icon="car", prefix="fa", color="blue", icon_color="white")

        # Add a marker with the car icon to the map
        folium.Marker(
            car_location,
            icon=car_icon,
            popup="Car Location"
        ).add_to(m)
        html(m._repr_html_(), height=500)

refresh_rate = 1000

# Auto-refresh
if time.time() - st.session_state.get("last_refresh", 0) > (refresh_rate / 1000):
    st.rerun()
st.session_state["last_refresh"] = time.time()

# Fix the layout
st.markdown("""
    <style>
        .block-container {
            display: flex;
            flex-direction: column;
            align-items: center;
        }
        .css-1kyxreq {
            justify-content: center;
        }
    </style>
""", unsafe_allow_html=True)
