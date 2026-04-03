import requests
from typing import Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def fetch_weather(city: str, country_code: str) -> Dict[str, Any]:
    if not API_KEY:
        raise ValueError("API_KEY is not set. Please set it in the .env file.")
    
    params = {
        "q": f"{city},{country_code}",
        "appid": API_KEY,
        "units": "metric"
    }

    resp = requests.get(BASE_URL, params=params, timeout=10)
    if resp.status_code != 200:
        resp.raise_for_status()

    return resp.json()