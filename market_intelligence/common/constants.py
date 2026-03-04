"""
common/constants.py

Project-wide constants. Import from here — never hardcode strings in app code.
"""

# ── Platform identifiers ───────────────────────────────────────────────────────
PLATFORM_BIGBASKET  = "bigbasket"
PLATFORM_BLINKIT    = "blinkit"
PLATFORM_ZEPTO      = "zepto"
PLATFORM_SWIGGY     = "swiggy"
PLATFORM_AMAZON     = "amazon"
PLATFORM_FLIPKART   = "flipkart"

ALL_PLATFORMS = [
    PLATFORM_BIGBASKET,
    PLATFORM_BLINKIT,
    PLATFORM_ZEPTO,
    PLATFORM_SWIGGY,
    PLATFORM_AMAZON,
    PLATFORM_FLIPKART,
]

# ── Ingestion pipeline names ───────────────────────────────────────────────────
PIPELINE_BIGBASKET_SCRAPER = "bigbasket_scraper"
PIPELINE_WEATHER           = "openweathermap"
PIPELINE_REDDIT_SENTIMENT  = "reddit"

# ── Monitoring statuses ────────────────────────────────────────────────────────
STATUS_SUCCESS = "success"
STATUS_PARTIAL = "partial"
STATUS_FAILED  = "failed"

# ── Forecasting models ─────────────────────────────────────────────────────────
MODEL_XGBOOST  = "xgboost"
MODEL_PROPHET  = "prophet"
MODEL_LSTM     = "lstm"
MODEL_ARIMA    = "arima"
MODEL_ENSEMBLE = "ensemble"

# ── Sentiment sources ──────────────────────────────────────────────────────────
SOURCE_REDDIT  = "reddit"
SOURCE_TWITTER = "twitter"
SOURCE_NEWSAPI = "newsapi"

# ── Default cities ─────────────────────────────────────────────────────────────
DEFAULT_CITIES = [
    "Mumbai", "Delhi", "Bangalore",
    "Chennai", "Kolkata", "Hyderabad", "Pune",
]

# ── Celery queue names ─────────────────────────────────────────────────────────
QUEUE_INGESTION  = "ingestion"
QUEUE_ML         = "ml"
QUEUE_DEFAULT    = "default"
