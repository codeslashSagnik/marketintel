import os, json, time, logging, traceback
from datetime import datetime, timedelta
from pathlib import Path
import requests
import pandas as pd

# ── Directories ──────────────────────────────────────────────
PROJECT_ROOT = Path("E:/cv projects/real_time-market-intelligence")
DATA_DIR     = PROJECT_ROOT / "data"
RAW_DIR      = DATA_DIR / "raw"
CACHE_DIR    = DATA_DIR / "cache"
LOGS_DIR     = PROJECT_ROOT / "logs"

for d in [RAW_DIR, CACHE_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────
def _get_logger() -> logging.Logger:
    log = logging.getLogger("weather_scraper")
    if log.handlers: return log
    log.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(LOGS_DIR / "weather_scraper.log", encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(ch)
    return log

logger = _get_logger()

# ── Configuration ──────────────────────────────────────────────
CITY_CONFIG = {
    "kolkata": {
        "central":     ["700001", "700020"],
        "residential": ["700064", "700091"],
        "peripheral":  ["700084", "700104"]
    },
    "mumbai": {
        "central":     ["400001", "400021"],
        "residential": ["400053", "400067"],
        "peripheral":  ["400706", "400709"]
    },
    "delhi": {
        "central":     ["110001", "110011"],
        "residential": ["110085", "110075"],
        "peripheral":  ["110041", "110043"]
    },
    "bangalore": {
        "central":     ["560001", "560025"],
        "residential": ["560037", "560102"],
        "peripheral":  ["560067", "560105"]
    },
    "pune": {
        "central":     ["411001", "411004"],
        "residential": ["411014", "411057"],
        "peripheral":  ["412105", "412308"]
    },
}

# ── Kafka Wrapper ──────────────────────────────────────────────
class KafkaProducerWrapper:
    def __init__(self, topic: str):
        self.topic = topic
        self.producer = None
        
        broker = os.environ.get("KAFKA_BROKER", "localhost:9092")
        if broker == "kafka:9092" and os.name == 'nt':
            logger.warning("KAFKA_BROKER set to 'kafka:9092' on Windows. Falling back to 'localhost:9092'.")
            broker = "localhost:9092"
            
        try:
            from confluent_kafka import Producer
            self.producer = Producer({
                "bootstrap.servers": broker,
                "client.id": "scraper-weather",
                "queue.buffering.max.messages": 10000,
                "batch.size": 32768,
                "linger.ms": 100,
                "compression.type": "snappy",
                "acks": "1",
            })
            logger.info(f"Connected to Kafka broker {broker} for topic {topic}")
        except ImportError:
            logger.error("confluent_kafka not installed. Kafka publishing disabled.")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")

    def _delivery_report(self, err, msg):
        if err is not None:
             logger.debug(f"Message delivery failed: {err}")

    def publish(self, record: dict):
        if not self.producer: return
        try:
            key = f"{record.get('city', 'unk')}_{record.get('pincode', 'unk')}"
            val = json.dumps(record).encode("utf-8")
            self.producer.produce(self.topic, key=key.encode("utf-8"), value=val, callback=self._delivery_report)
            self.producer.poll(0)
        except Exception as e:
            logger.warning(f"Kafka publish error: {e}")

    def flush(self):
        if self.producer:
            self.producer.flush()

    def close(self):
        if self.producer:
            self.flush()


# ── Core Logic ──────────────────────────────────────────────
class WeatherScraper:
    def __init__(self):
        self.geocode_cache_file = CACHE_DIR / "geocode_cache.json"
        self.geocode_cache = self._load_cache()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MarketIntelligenceWeatherBot/1.0"})
        self.kafka = KafkaProducerWrapper(topic="context.weather")

    def _load_cache(self) -> dict:
        if self.geocode_cache_file.exists():
            try:
                with open(self.geocode_cache_file, "r") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_cache(self):
        with open(self.geocode_cache_file, "w") as f:
            json.dump(self.geocode_cache, f, indent=2)

    def geocode_pincode(self, city: str, pincode: str) -> tuple:
        """Get Latitude and Longitude for a given pincode and city."""
        cache_key = f"{city}_{pincode}"
        if cache_key in self.geocode_cache:
            return self.geocode_cache[cache_key]["lat"], self.geocode_cache[cache_key]["lon"]

        query = f"{pincode}, {city}, India"
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": query, "format": "json", "limit": 1}
        
        try:
            logger.info(f"Geocoding {query} via OpenStreetMap...")
            resp = self.session.get(url, params=params, timeout=10)
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
                logger.warning(f"Geocoding failed for {query} - No results.")
        except Exception as e:
            logger.error(f"Geocoding error for {query}: {e}")
        
        time.sleep(1.5)
        return None, None

    def fetch_weather_data(self, lat: float, lon: float) -> dict:
        """Fetch historical and forecast data from Open-Meteo."""
        # 3 days past, today, 3 days forecast = total 7 days
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
            "target": "models" # ensure uniform format
        }

        try:
            resp = self.session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Weather API error for {lat},{lon}: {e}")
            return None

    def map_weather_payload(self, city: str, zone: str, pincode: str, lat: float, lon: float, api_data: dict) -> dict:
        """Map the Open-Meteo response into our unified JSON schema."""
        if not api_data or "daily" not in api_data:
            return None

        daily = api_data["daily"]
        dates = daily.get("time", [])
        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])
        precip = daily.get("precipitation_sum", [])
        wind = daily.get("wind_speed_10m_max", [])
        
        # Open-Meteo doesn't give daily avg humidity in the standard free tier easily without hourly agg, 
        # so we will leave it out or mock absent if we just want the other 4 core metrics.
        # Wind, Precip, Max/Min Temp are present.
        
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

    def run(self, city_config: dict):
        total_pincodes_processed = 0
        total_success = 0
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        valid_records = []

        for city, zones in city_config.items():
            for zone, pincodes in zones.items():
                for pincode in pincodes:
                    total_pincodes_processed += 1
                    logger.info(f"Processing weather for {city} | {zone} | {pincode}")
                    
                    # 1. Geocode
                    lat, lon = self.geocode_pincode(city, pincode)
                    if lat is None or lon is None:
                        continue

                    # 2. Fetch Weather
                    api_data = self.fetch_weather_data(lat, lon)
                    if not api_data:
                        continue
                    
                    # 3. Map Data
                    payload = self.map_weather_payload(city, zone, pincode, lat, lon, api_data)
                    if payload:
                        valid_records.append(payload)
                        # Publish to Kafka
                        self.kafka.publish(payload)
                        total_success += 1

        self.kafka.close()

        # 4. Save batch to disk as JSON Lines
        if valid_records:
            out_path = RAW_DIR / f"weather_all_{timestamp}.jsonl"
            with open(out_path, "w", encoding="utf-8") as f:
                for r in valid_records:
                    f.write(json.dumps(r) + "\n")
            logger.info(f"Pipeline finished. Saved {total_success}/{total_pincodes_processed} pincodes to {out_path}")
        else:
            logger.warning("Pipeline finished with NO records.")


if __name__ == "__main__":
    t0 = time.monotonic()
    scraper = WeatherScraper()
    scraper.run(CITY_CONFIG)
    logger.info(f"Completed in {time.monotonic() - t0:.1f} seconds")
