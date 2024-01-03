import os
import requests
from dotenv import load_dotenv
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Load API key and database connection string from environment variables
api_key = os.environ.get("OPENWEATHER_API_KEY")
db_connection_string = os.environ.get("DB_CONNECTION_STRING")

# SQLite database connection
conn = sqlite3.connect(db_connection_string)
cursor = conn.cursor()

# Check if the weather_data table exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weather_data'")
table_exists = cursor.fetchone()

if not table_exists:
    # Create a new weather_data table with the required schema
    cursor.execute('''
        CREATE TABLE weather_data (
            id INTEGER PRIMARY KEY,
            city TEXT,
            temperature_avg REAL,
            humidity INTEGER,
            precipitation REAL,
            population INTEGER,
            timestamp TEXT
        )
    ''')

    # Commit the create statement
    conn.commit()

def extract(city):
    # Make API request for weather data
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"
    response = requests.get(url)
    data = response.json()
    return data

def transform(data, population):
    # Extract relevant data
    temperature_sum = 0
    humidity_sum = 0
    precipitation_sum = 0

    # Counters for hourly data
    count = 0

    # Loop through hourly data to calculate daily averages
    for entry in data.get("list", []):
        temperature_sum += entry.get("main", {}).get("temp", 0)
        humidity_sum += entry.get("main", {}).get("humidity", 0)
        precipitation_sum += entry.get("rain", {}).get("3h", 0)
        count += 1

    # Calculate daily averages
    temperature_avg = round(temperature_sum / count - 273.15, 2) if count > 0 else 0.0
    humidity_avg = humidity_sum / count if count > 0 else 0.0
    precipitation_avg = precipitation_sum / count if count > 0 else 0.0

    # Normalize temperature to Celsius
    # Handle missing or null values
    if temperature_avg is None:
        temperature_avg = 0.0

    # Enrich data with population
    if population is not None:
        data["population"] = population

    return {
        "city": data.get("city", {}).get("name"),
        "temperature_avg": temperature_avg,
        "humidity": round(humidity_avg),
        "precipitation": precipitation_avg,
        "population": population,
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    }

def load(data):
    # Insert data into the SQLite database using placeholders
    cursor.execute('''
        INSERT INTO weather_data (city, temperature_avg, humidity, precipitation, population, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (data["city"], data["temperature_avg"], data["humidity"], data["precipitation"], data["population"], data["timestamp"]))

def is_rain_forecasted(forecast_data):
    # Check if rain is forecasted for the next day
    for entry in forecast_data.get("list", []):
        timestamp = entry.get("dt_txt")
        date = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        tomorrow = datetime.utcnow() + timedelta(days=1)
        if date.date() == tomorrow.date() and entry.get("weather", [])[0].get("main") == "Rain":
            return True
    return False

def get_city_population(city):
    # Read population data from CSV
    population_df = pd.read_csv("worldcities.csv", usecols=["city", "population"])

    # Find population data for the city
    city_population = population_df[population_df["city"] == city]

    if not city_population.empty:
        return city_population.iloc[0]["population"]
    else:
        return None

# List of cities
cities = ["London", "Leeds", "Nottingham", "Manchester", "Bangalore"]

# Loop through cities and perform ETL
for city in cities:
    # Extract data
    weather_data = extract(city)

    # Get population data for the city
    city_population_value = get_city_population(city)

    # Transform data
    transformed_data = transform(weather_data, city_population_value)

    # Load data into the database
    load(transformed_data)

    # Print message based on rain forecast
    if is_rain_forecasted(extract(city)):
        print(f"Rain forecasted for {city} tomorrow!")
    else:
        print(f"No rain forecast for {city} tomorrow.")

# Commit the changes and close the connection
conn.commit()
conn.close()
