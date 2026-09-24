import os
import requests
from dotenv import load_dotenv


# Load variables from .env
load_dotenv()

# Get the API key from .env
api_key = os.getenv("TWELVE_DATA_API_KEY")

# Check whether the API key was loaded
if not api_key:
    raise RuntimeError("TWELVE_DATA_API_KEY is missing from .env")


# Twelve Data API endpoint
url = "https://api.twelvedata.com/price"

# Parameters sent to the API
params = {
    "symbol": "AAPL",
    "apikey": api_key
}


# Send request to Twelve Data
response = requests.get(
    url,
    params=params,
    timeout=10
)


# Display the response
print("HTTP status:", response.status_code)

print("Response:")
print(response.json())
