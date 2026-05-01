import os
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from datetime import datetime

load_dotenv()

# PostgreSQL connection
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")

engine = create_engine(
    f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "new_weather_data"
MONGO_COLLECTION = "weather"

mongo_client = MongoClient(MONGO_URI)
collection = mongo_client[MONGO_DB][MONGO_COLLECTION]


def create_table():
    query = """
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
        UNIQUE(city, country, observed_at)
    );
    """
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()


def fetch_from_mongo():
    return list(collection.find())


def transform(doc):
    return {
        "city": doc.get("name"),
        "country": doc.get("sys", {}).get("country"),
        "longitude": doc.get("coord", {}).get("lon"),
        "latitude": doc.get("coord", {}).get("lat"),
        "temperature": doc.get("main", {}).get("temp"),
        "humidity": doc.get("main", {}).get("humidity"),
        "pressure": doc.get("main", {}).get("pressure"),
        "wind_speed": doc.get("wind", {}).get("speed"),
        "wind_direction": doc.get("wind", {}).get("deg"),
        "observed_at": datetime.utcfromtimestamp(doc.get("dt")),
        "provider": "openweathermap"
    }


def insert_into_pg(rows):
    query = """
    INSERT INTO weather_observations (
        city, country, longitude, latitude, temperature,
        humidity, pressure, wind_speed, wind_direction,
        observed_at, provider
    )
    VALUES (
        :city, :country, :longitude, :latitude, :temperature,
        :humidity, :pressure, :wind_speed, :wind_direction,
        :observed_at, :provider
    )
    ON CONFLICT (city, country, observed_at) DO NOTHING;
    """

    with engine.connect() as conn:
        for row in rows:
            conn.execute(text(query), row)
        conn.commit()


def run_pg_ingestion():
    create_table()
    docs = fetch_from_mongo()
    rows = [transform(doc) for doc in docs]
    insert_into_pg(rows)
