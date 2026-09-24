import os
import json
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer


# Load environment variables from .env
load_dotenv()

# Get Twelve Data API key
api_key = os.getenv("TWELVE_DATA_API_KEY")

if not api_key:
    raise RuntimeError("TWELVE_DATA_API_KEY is missing from .env")


# Twelve Data API
API_URL = "https://api.twelvedata.com/price"

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "market_ticks"

# Market symbol
SYMBOL = "AAPL"


# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


print(f"Starting market data producer for {SYMBOL}...")
print(f"Sending events to Kafka topic: {KAFKA_TOPIC}")


try:
    while True:

        # Request current price from Twelve Data
        params = {
            "symbol": SYMBOL,
            "apikey": api_key
        }

        response = requests.get(
            API_URL,
            params=params,
            timeout=10
        )

        response.raise_for_status()

        data = response.json()

        # Check API response
        if "price" not in data:
            print("Unexpected API response:", data)
            time.sleep(10)
            continue

        # Create our Kafka event
        event = {
            "symbol": SYMBOL,
            "price": float(data["price"]),
            "event_time": datetime.now(timezone.utc).isoformat()
        }

        # Send event to Kafka
        producer.send(KAFKA_TOPIC, value=event)

        # Make sure the message is sent
        producer.flush()

        print("Sent:", event)

        # Wait before requesting again
        time.sleep(10)


except KeyboardInterrupt:
    print("\nStopping producer...")


finally:
    producer.close()
    print("Kafka producer closed.")
