from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGODB_NAME")
COLLECTION_NAME = os.getenv("MONGODB_COLLECTION_NAME")

_client = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(URI)
    return _client

def get_collection():
    client = get_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    return collection


# Libraries and Modules.
# Packages and Dependencies.