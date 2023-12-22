import os
import requests
from dotenv import load_dotenv
import pandas as pd
import sqlite3

# Load environment variables from .env file
load_dotenv()

# Load API key from environment variable
api_key = os.environ.get("OPENWEATHER_API_KEY")

# SQLite database connection string
db_connection_string = os.environ.get("DB_CONNECTION_STRING")
conn = sqlite3.connect(db_connection_string)
cursor = conn.cursor()

# List of cities
cities = ["London", "Leeds", "Nottingham", "Manchester"]

# Read population data from CSV
population_df = pd.read_csv("worldcities.csv", usecols=["city", "population"])

# Create a table to store weather data
cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        id INTEGER PRIMARY KEY,
        city TEXT,
        temperature_celsius REAL,
        humidity INTEGER,
        population INTEGER
    )
''')
conn.commit()

# Loop through cities and make API requests
for city in cities:
    city_data = {
        "City": city,
    }

    # Find population data for the city
    city_population = population_df[population_df["city"] == city]

    if not city_population.empty:
        city_data["Population"] = city_population.iloc[0]["population"]

    # Make API request for weather data
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    data = response.json()

    # Extract relevant data
    city_data["Temperature_Celsius"] = round(data.get("main", {}).get("temp") - 273.15, 2)
    city_data["Humidity"] = data.get("main", {}).get("humidity")

    # Insert data into the SQLite database using placeholders
    cursor.execute('''
        INSERT INTO weather_data (city, temperature_celsius, humidity, population)
        VALUES (?, ?, ?, ?)
    ''', (city_data["City"], city_data["Temperature_Celsius"], city_data["Humidity"], city_data["Population"]))

# Commit the changes and close the connection
conn.commit()
conn.close()
