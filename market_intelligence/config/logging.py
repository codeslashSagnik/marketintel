"""
config/logging.py

Production-grade structured logging configuration.
Format: timestamp | level | service | module | message
"""

import os

LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,

    # ── Formatters ─────────────────────────────────────────────────────────────
    "formatters": {
        "verbose": {
            "format": "{asctime} | {levelname:<8} | {name} | {module} | {message}",
            "style": "{",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {
            "format": "{levelname} {message}",
            "style": "{",
        },
    },

    # ── Filters ────────────────────────────────────────────────────────────────
    "filters": {
        "require_debug_false": {
            "()": "django.utils.log.RequireDebugFalse",
        },
    },

    # ── Handlers ───────────────────────────────────────────────────────────────
    "handlers": {
        # Console handler for development visibility
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "verbose",
            "level": "DEBUG",
        },

        # General Django application log
        "django_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "django.log"),
            "maxBytes": 10 * 1024 * 1024,  # 10 MB
            "backupCount": 5,
            "formatter": "verbose",
            "level": "INFO",
            "encoding": "utf-8",
        },

        # Data ingestion pipeline log
        "ingestion_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "ingestion.log"),
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "formatter": "verbose",
            "level": "INFO",
            "encoding": "utf-8",
        },

        # Celery background workers & beat scheduler log
        "celery_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "celery.log"),
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 5,
            "formatter": "verbose",
            "level": "INFO",
            "encoding": "utf-8",
        },

        # Errors-only log (all services)
        "error_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": os.path.join(LOGS_DIR, "errors.log"),
            "maxBytes": 10 * 1024 * 1024,
            "backupCount": 10,
            "formatter": "verbose",
            "level": "ERROR",
            "encoding": "utf-8",
        },
    },

    # ── Loggers ────────────────────────────────────────────────────────────────
    "loggers": {
        # Root Django logger
        "django": {
            "handlers": ["console", "django_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Ingestion app
        "apps.ingestion": {
            "handlers": ["console", "ingestion_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Weather app
        "apps.weather": {
            "handlers": ["console", "ingestion_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Sentiment app
        "apps.sentiment": {
            "handlers": ["console", "ingestion_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # ETL / feature engineering
        "apps.etl": {
            "handlers": ["console", "django_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Forecasting / ML
        "apps.forecasting": {
            "handlers": ["console", "django_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Products
        "apps.products": {
            "handlers": ["console", "django_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Sales
        "apps.sales": {
            "handlers": ["console", "django_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Monitoring
        "apps.monitoring": {
            "handlers": ["console", "django_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Celery worker / beat
        "celery": {
            "handlers": ["console", "celery_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },
        "celery.task": {
            "handlers": ["console", "celery_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Services layer (scrapers, API clients, ML)
        "services": {
            "handlers": ["console", "ingestion_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },

        # Common utilities
        "common": {
            "handlers": ["console", "django_file", "error_file"],
            "level": "INFO",
            "propagate": False,
        },
    },

    # ── Root logger fallback ───────────────────────────────────────────────────
    "root": {
        "handlers": ["console", "error_file"],
        "level": "WARNING",
    },
}
