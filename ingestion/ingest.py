import requests
import os
from dotenv import load_dotenv
from db import get_connection, create_raw_table

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")
BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

SERIES = {
    "GDP":    "Gross Domestic Product",
    "CPIAUCSL": "CPI - Inflation",
    "UNRATE": "Unemployment Rate",
    "FEDFUNDS": "Federal Funds Rate",
}

def fetch_series(series_id):
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "2000-01-01",
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()["observations"]

def load_to_db(conn, series_id, observations):
    with conn.cursor() as cur:
        for obs in observations:
            if obs["value"] == ".":   # FRED uses "." for missing values
                continue
            cur.execute("""
                INSERT INTO raw_fred_observations (series_id, obs_date, value)
                VALUES (%s, %s, %s)
                ON CONFLICT (series_id, obs_date) DO NOTHING;
            """, (series_id, obs["date"], float(obs["value"])))
    conn.commit()

if __name__ == "__main__":
    conn = get_connection()
    create_raw_table(conn)

    for series_id, label in SERIES.items():
        print(f"Fetching {label} ({series_id})...")
        observations = fetch_series(series_id)
        load_to_db(conn, series_id, observations)
        print(f"  → Loaded {len(observations)} rows")

    conn.close()
    print("Done.")
