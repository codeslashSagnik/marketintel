"""
services/api_clients/weather_client.py

Open-Meteo API client with OpenStreetMap geocoding fallback.
Fetches 7-day weather payloads (historical, current, forecast) for given pincodes.
"""
import os
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import requests

import logging

logger = logging.getLogger("services.api_clients.weather")

PROJECT_ROOT = Path("E:/cv projects/real_time-market-intelligence")
CACHE_DIR    = PROJECT_ROOT / "data" / "cache"

# Ensure cache directory exists
CACHE_DIR.mkdir(parents=True, exist_ok=True)


class WeatherClient:
    """
    Open-Meteo client that first geocodes a Pincode -> Lat/Lon
    using Nominatim (OpenStreetMap), then fetches historical/forecast data.
    """
    TIMEOUT = 10

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MarketIntelligenceWeatherBot/1.0"})
        self.geocode_cache_file = CACHE_DIR / "geocode_cache.json"
        self.geocode_cache = self._load_cache()

    def _load_cache(self) -> dict:
        if self.geocode_cache_file.exists():
            try:
                with open(self.geocode_cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_cache(self):
        with open(self.geocode_cache_file, "w", encoding="utf-8") as f:
            json.dump(self.geocode_cache, f, indent=2)

    def geocode_pincode(self, city: str, pincode: str) -> tuple[float | None, float | None]:
        """Get Latitude and Longitude for a given pincode and city."""
        cache_key = f"{city}_{pincode}"
        if cache_key in self.geocode_cache:
            return self.geocode_cache[cache_key]["lat"], self.geocode_cache[cache_key]["lon"]

        query = f"{pincode}, {city}, India"
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": query, "format": "json", "limit": 1}
        
        try:
            logger.info("Geocoding %s via OpenStreetMap...", query)
            resp = self.session.get(url, params=params, timeout=self.TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                self.geocode_cache[cache_key] = {"lat": lat, "lon": lon}
                self._save_cache()
                time.sleep(1.5) # Nominatim policy: max 1 req / sec
                return lat, lon
            else:
                logger.warning("Geocoding failed for %s - No results.", query)
        except Exception as e:
            logger.error("Geocoding error for %s: %s", query, e)
        
        time.sleep(1.5)
        return None, None

    def fetch_weather_7_days(self, city: str, zone: str, pincode: str) -> dict | None:
        """
        Main entry point. Geocodes the request, hits Open-Meteo, 
        and maps output to the target schema.
        """
        lat, lon = self.geocode_pincode(city, pincode)
        if lat is None or lon is None:
            return None

        today = datetime.utcnow().date()
        start_date = today - timedelta(days=3)
        end_date = today + timedelta(days=3)

        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
            "timezone": "auto",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "target": "models"
        }

        try:
            resp = self.session.get(url, params=params, timeout=self.TIMEOUT)
            resp.raise_for_status()
            api_data = resp.json()
            return self._map_payload(city, zone, pincode, lat, lon, api_data)
        except Exception as e:
            logger.error("Weather API error for %s,%s: %s", lat, lon, e)
            return None

    def _map_payload(self, city: str, zone: str, pincode: str, lat: float, lon: float, api_data: dict) -> dict | None:
        """Map the Open-Meteo response into our unified JSON schema."""
        if not api_data or "daily" not in api_data:
            return None

        daily = api_data["daily"]
        dates = daily.get("time", [])
        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])
        precip = daily.get("precipitation_sum", [])
        wind = daily.get("wind_speed_10m_max", [])
        
        today_str = datetime.utcnow().date().isoformat()
        weather_list = []

        for i in range(len(dates)):
            date_str = dates[i]
            
            if date_str < today_str:
                day_type = "historical"
            elif date_str == today_str:
                day_type = "today"
            else:
                day_type = "forecast"

            weather_list.append({
                "date": date_str,
                "type": day_type,
                "temp_max_c": temp_max[i] if i < len(temp_max) else None,
                "temp_min_c": temp_min[i] if i < len(temp_min) else None,
                "precipitation_mm": precip[i] if i < len(precip) else None,
                "wind_speed_kmh": wind[i] if i < len(wind) else None
            })

        payload = {
            "source": "open_meteo",
            "city": city,
            "zone": zone,
            "pincode": pincode,
            "lat": lat,
            "lon": lon,
            "scraped_at": datetime.utcnow().isoformat() + "Z",
            "weather_data": weather_list
        }
        return payload

