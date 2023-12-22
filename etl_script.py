import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load API key from environment variable
api_key = os.environ.get("OPENWEATHER_API_KEY")

# Make API request
url = f"https://api.openweathermap.org/data/2.5/weather?q=Leeds&appid={api_key}"
response = requests.get(url)
data = response.json()

# Implement your data transformation logic here

# Example: Print the temperature
temperature = data.get("main", {}).get("temp")
print(f"Temperature: {temperature} K")
