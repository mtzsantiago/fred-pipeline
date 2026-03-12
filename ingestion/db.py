import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

def create_raw_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_fred_observations (
                id SERIAL PRIMARY KEY,
                series_id VARCHAR(50) NOT NULL,
                obs_date DATE NOT NULL,
                value NUMERIC,
                ingested_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(series_id, obs_date)
            );
        """)
    conn.commit()
