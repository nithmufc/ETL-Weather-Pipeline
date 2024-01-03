import os
import requests
from dotenv import load_dotenv
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(filename='etl_log.txt', level=logging.ERROR)

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
    try:
        # Make API request for weather data
        url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        data = response.json()
        return data
    except requests.RequestException as e:
        logging.error(f"Error retrieving data for {city} from the API: {e}")
        return None

def transform(data, population):
    try:
        # Extract relevant data
        hourly_data = data.get("list", [])
        count = len(hourly_data)

        # Initialize sums
        temperature_sum = 0
        humidity_sum = 0
        precipitation_sum = 0

        # Loop through hourly data to calculate sums
        for entry in hourly_data:
            temperature_sum += entry.get("main", {}).get("temp", 0)
            humidity_sum += entry.get("main", {}).get("humidity", 0)
            precipitation_sum += entry.get("rain", {}).get("3h", 0)

        # Calculate daily averages
        temperature_avg = round((temperature_sum / count) - 273.15, 2) if count > 0 else 0.0
        humidity_avg = round(humidity_sum / count, 2) if count > 0 else 0.0
        precipitation_avg = round(precipitation_sum / count, 2) if count > 0 else 0.0

        # Normalize temperature to Celsius
        # Handle missing or null values
        if temperature_avg is None:
            temperature_avg = 0.0

        # Enrich data with population
        if population is not None:
            data["population"] = population

        result = {
            "city": data.get("city", {}).get("name"),
            "temperature_avg": temperature_avg,
            "humidity": humidity_avg,
            "precipitation": precipitation_avg,
            "population": population,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        }

        return result

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        return None

def load(data):
    try:
        # Insert data into the SQLite database using placeholders
        cursor.execute('''
            INSERT INTO weather_data (city, temperature_avg, humidity, precipitation, population, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (data["city"], data["temperature_avg"], data["humidity"], data["precipitation"], data["population"], data["timestamp"]))
        conn.commit()
    except sqlite3.Error as e:
        conn.rollback()  # Rollback the transaction in case of an error
        logging.error(f"Error writing to the database: {e}")

def is_rain_forecasted(forecast_data):
    try:
        # Check if rain is forecasted for the next day
        for entry in forecast_data.get("list", []):
            timestamp = entry.get("dt_txt")
            date = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            tomorrow = datetime.utcnow() + timedelta(days=1)
            if date.date() == tomorrow.date() and entry.get("weather", [])[0].get("main") == "Rain":
                return True
    except Exception as e:
        logging.error(f"Error checking rain forecast: {e}")

    return False

def get_city_population(city):
    try:
        # Read population data from CSV
        population_df = pd.read_csv("worldcities.csv", usecols=["city", "population"])

        # Find population data for the city
        city_population = population_df[population_df["city"] == city]

        if not city_population.empty:
            return city_population.iloc[0]["population"]
        else:
            return None
    except Exception as e:
        logging.error(f"Error while retrieving population data for {city}: {e}")
        return None

# List of cities
cities = ["London", "Leeds", "Nottingham", "Manchester", "Bangalore"]

# Loop through cities and perform ETL
for city in cities:
    # Extract data
    weather_data = extract(city)

    if weather_data is not None:
        # Get population data for the city
        city_population_value = get_city_population(city)

        # Transform data
        transformed_data = transform(weather_data, city_population_value)

        if transformed_data is not None:
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
