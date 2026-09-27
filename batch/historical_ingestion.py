import logging
import os
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

api_key = os.getenv("TWELVE_DATA_API_KEY")

if not api_key:
    raise RuntimeError("TWELVE_DATA_API_KEY is missing from .env")

API_URL = "https://api.twelvedata.com/time_series"

params = {
    "symbol": "AAPL",
    "interval": "1min",
    "outputsize": 500,
    "apikey": api_key
}

try:
    logger.info("Requesting historical market data")

    response = requests.get(
        API_URL,
        params=params,
        timeout=20
    )

    response.raise_for_status()

    data = response.json()

    if "values" not in data:
        raise RuntimeError(f"Unexpected API response: {data}")

    records = data["values"]

    output_dir = "data/batch_bronze/market_history"
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(
        output_dir,
        f"AAPL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )

    with open(output_file, "w", encoding="utf-8") as file:
        file.write("symbol,datetime,open,high,low,close,volume\n")

        for record in records:
            file.write(
                f"AAPL,"
                f"{record['datetime']},"
                f"{record['open']},"
                f"{record['high']},"
                f"{record['low']},"
                f"{record['close']},"
                f"{record['volume']}\n"
            )

    logger.info("Symbol: %s", data["meta"]["symbol"])
    logger.info("Interval: %s", data["meta"]["interval"])
    logger.info("Records received: %d", len(records))
    logger.info("Bronze file written: %s", output_file)

except requests.RequestException as error:
    logger.error("Market data API request failed: %s", error)
    raise

except Exception as error:
    logger.error("Historical ingestion failed: %s", error)
    raise
