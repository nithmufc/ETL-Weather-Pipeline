Weather ETL Script

Overview

This script fetches current weather data for cities, cleans it up, and stores it in a simple SQLite database. It also adds extra info, like city population, from a CSV file.

Usage
Set-Up:

Add your OpenWeatherMap API key and SQLite database connection string to a .env file.

Run the Script:

Execute etl_script.py to get weather data, tidy it up, and save it to the database.

Data Enrichment

The script enriches weather data by including additional city population details from a worldcities.csv file.
