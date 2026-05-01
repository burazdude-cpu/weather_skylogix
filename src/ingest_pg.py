
# Get data from MongoDB, transform it, and write to PostgreSQL

# Columns: city, country, longitude, latitude, temperature, humidity, pressure, wind_speed, wind_direction,
# Observed_at, provider.

# Create a table in PostgreSQL
# Write the clean data to PostgreSQL, ensuring no duplicates based on city, country, and observed_at timestamp.

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

from .mongo_client import get_collection

load_dotenv()


def get_pg_engine():
    """Create PostgreSQL engine"""
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    db = os.getenv("PG_DATABASE")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def create_table(engine):
    """Create table with unique constraint"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS weather_observations (
        city TEXT,
        country TEXT,
        longitude FLOAT,
        latitude FLOAT,
        temperature FLOAT,
        humidity FLOAT,
        pressure FLOAT,
        wind_speed FLOAT,
        wind_direction FLOAT,
        observed_at TIMESTAMP,
        provider TEXT,
        UNIQUE (city, country, observed_at)
    );
    """

    with engine.connect() as conn:
        conn.execute(text(create_table_query))
        conn.commit()


def fetch_mongo_documents():
    """Fetch all documents from MongoDB"""
    col = get_collection()
    return list(col.find({}))


def transform_document(doc):
    """Flatten MongoDB document into PostgreSQL row"""

    return {
        "city": doc.get("city"),
        "country": doc.get("country_code"),
        "longitude": doc["coordinates"]["lon"],
        "latitude": doc["coordinates"]["lat"],
        "temperature": doc["metrics"]["temperature"],
        "humidity": doc["metrics"]["humidity"],
        "pressure": doc["metrics"]["pressure"],
        "wind_speed": doc["metrics"]["wind_speed"],
        "wind_direction": doc["metrics"]["wind_direction"],
        "observed_at": doc.get("observed_at"),
        "provider": doc.get("provider"),
    }


def insert_into_postgres(engine, rows):
    """Insert rows using ON CONFLICT DO NOTHING"""

    insert_query = text("""
        INSERT INTO weather_observations (
            city, country, longitude, latitude,
            temperature, humidity, pressure,
            wind_speed, wind_direction,
            observed_at, provider
        )
        VALUES (
            :city, :country, :longitude, :latitude,
            :temperature, :humidity, :pressure,
            :wind_speed, :wind_direction,
            :observed_at, :provider
        )
        ON CONFLICT (city, country, observed_at) DO NOTHING;
    """)

    inserted = 0

    with engine.connect() as conn:
        for row in rows:
            result = conn.execute(insert_query, row)
            inserted += result.rowcount

        conn.commit()

    print(f"Inserted {inserted} new rows (duplicates skipped).")


def run_pg_ingestion():
    """Main function"""
    engine = get_pg_engine()

    print("Creating table if not exists...")
    create_table(engine)

    print("Fetching data from MongoDB...")
    docs = fetch_mongo_documents()

    if not docs:
        print("No documents found.")
        return

    print(f"Transforming {len(docs)} documents...")
    rows = [transform_document(doc) for doc in docs]

    print("Inserting into PostgreSQL...")
    insert_into_postgres(engine, rows)

    print("PostgreSQL ingestion complete.")
