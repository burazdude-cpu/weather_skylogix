from datetime import datetime
from typing import Dict, Any

PROVIDER = "openweathermap"

def normalize_weather_data(raw_data: Dict[str, Any],
                           city: str,
                           country_code: str) -> Dict[str, Any]:
    
    observed_at_df = datetime.fromtimestamp(raw_data["dt"])
    print(f"Observed at (datetime): {observed_at_df}")

    doc = {
        "city": city,
        "country_code": country_code,
        "coordinates": {
            "lat": raw_data["coord"]["lat"],
            "lon": raw_data["coord"]["lon"]
        },
        "provider": PROVIDER,
        "observed_at": observed_at_df,
        "metrics": {
            "temperature": raw_data["main"]["temp"],
            "humidity": raw_data["main"]["humidity"],
            "pressure": raw_data["main"]["pressure"],
            "wind_speed": raw_data["wind"]["speed"],
            "wind_direction": raw_data["wind"]["deg"],
        },
        "conditions": {
            "description": raw_data["weather"][0]["description"],
            "icon": raw_data["weather"][0]["icon"]
        },
        "raw_data": raw_data
    }

    return doc

