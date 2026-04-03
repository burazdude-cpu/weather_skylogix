from typing import List
from .mongo_client import get_collection
from .weather_client import fetch_weather
from .normalize import normalize_weather_data
from pymongo import UpdateOne, ASCENDING, errors as pymongo_errors


CITIES = [
    {"city": "New York", "country_code": "US"},
    {"city": "London", "country_code": "GB"},
    {"city": "Tokyo", "country_code": "JP"},
    {"city": "Sydney", "country_code": "AU"},
    {"city": "Mumbai", "country_code": "IN"},
]


def ensure_indexes():
    col = get_collection()
    col.create_index("updatedAt")
    col.create_index([("city", ASCENDING), ("observed_at", ASCENDING)])


def ingest_once(cities: List[dict]):
    if cities is None:
        cities = CITIES

    if not cities:
        print("No cities provided for ingestion.")
        return
    
    col  = get_collection()

    operations = []

    for c in cities:
        city = c["city"]
        country_code = c["country_code"]
        print(f"Fetching weather for {city}, {country_code}...")

        try:
            raw_data = fetch_weather(city, country_code)
            normalized_doc = normalize_weather_data(raw_data, city, country_code)

            filter_query = {
                "city": city,
                "country_code": country_code,
                "observed_at": normalized_doc["observed_at"]
            }

            update_doc = {
                "$set": normalized_doc,
                "$currentDate": {"updatedAt": True}
            }
            operations.append(UpdateOne(filter_query, update_doc, upsert=True))
        except Exception as e:
            print(f"Error fetching or normalizing data for {city}, {country_code}: {e}")

    if operations:
        try:
            result = col.bulk_write(operations)
            print(f"Bulk write result: {result.bulk_api_result}")
        except pymongo_errors.BulkWriteError as bwe:
            print(f"Bulk write error: {bwe.details}")
        except Exception as e:
            print(f"Error during bulk write: {e}")
            