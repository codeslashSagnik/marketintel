"""
config/settings.py

Production-ready Django settings for the Market Intelligence Platform.
All sensitive values are read from environment variables via python-dotenv.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from config.logging import LOGGING  # noqa: F401  (imported to make it available)

# ── Environment ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "changeme-in-production")

DEBUG = os.environ.get("DEBUG", "True") == "True"

ALLOWED_HOSTS = os.environ.get("ALLOWED_HOSTS", "localhost 127.0.0.1").split()


# ── Installed Apps ─────────────────────────────────────────────────────────────
INSTALLED_APPS = [
    # Django internals
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",

    # Third-party
    "rest_framework",
    "django_celery_beat",
    "django_celery_results",

    # Project apps
    "apps.products",
    "apps.ingestion",
    "apps.weather",
    "apps.sentiment",
    "apps.sales",
    "apps.etl",
    "apps.forecasting",
    "apps.monitoring",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"


# ── Database (PostgreSQL) ──────────────────────────────────────────────────────
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME":     os.environ.get("DB_NAME",     "market_intelligence"),
        "USER":     os.environ.get("DB_USER",     "postgres"),
        "PASSWORD": os.environ.get("DB_PASSWORD", "postgres"),
        "HOST":     os.environ.get("DB_HOST",     "localhost"),
        "PORT":     os.environ.get("DB_PORT",     "5432"),
        "CONN_MAX_AGE": 60,  # persistent connections
        "OPTIONS": {
            "sslmode": os.environ.get("DB_SSL_MODE", "prefer"),
        },
    }
}


# ── Cache & Sessions ───────────────────────────────────────────────────────────
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": os.environ.get("REDIS_URL", "redis://localhost:6379/1"),
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient",
        },
    }
}


# ── Celery ─────────────────────────────────────────────────────────────────────
CELERY_BROKER_URL          = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_RESULT_BACKEND      = "django-db"           # stored in PostgreSQL via django-celery-results
CELERY_ACCEPT_CONTENT      = ["json"]
CELERY_TASK_SERIALIZER     = "json"
CELERY_RESULT_SERIALIZER   = "json"
CELERY_TIMEZONE            = "Asia/Kolkata"
CELERY_ENABLE_UTC          = True
CELERY_BEAT_SCHEDULER      = "django_celery_beat.schedulers:DatabaseScheduler"

# Task-level settings
CELERY_TASK_TRACK_STARTED  = True
CELERY_TASK_TIME_LIMIT     = 5 * 60        # 5-minute hard limit per task
CELERY_TASK_SOFT_TIME_LIMIT = 4 * 60      # 4-minute soft limit
CELERY_TASK_MAX_RETRIES    = 3


# ── Django REST Framework ──────────────────────────────────────────────────────
REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
    ],
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 100,
}


# ── Internationalization ───────────────────────────────────────────────────────
LANGUAGE_CODE = "en-us"
TIME_ZONE     = "Asia/Kolkata"
USE_I18N      = True
USE_TZ        = True


# ── Static Files ───────────────────────────────────────────────────────────────
STATIC_URL  = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"


# ── Default Primary Key ────────────────────────────────────────────────────────
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


# ── Logging  (pulled from config/logging.py) ───────────────────────────────────
LOGGING = LOGGING


# ── External API Keys ──────────────────────────────────────────────────────────
OPENWEATHERMAP_API_KEY = os.environ.get("OPENWEATHERMAP_API_KEY", "")
REDDIT_CLIENT_ID       = os.environ.get("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET   = os.environ.get("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT      = os.environ.get("REDDIT_USER_AGENT", "market-intelligence-bot/1.0")
