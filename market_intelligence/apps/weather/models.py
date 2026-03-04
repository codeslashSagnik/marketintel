"""
apps/weather/models.py

Meteorological readings ingested from OpenWeatherMap or similar APIs.
Weather data is used as a feature signal for demand forecasting.
"""
import logging
from django.db import models

logger = logging.getLogger(__name__)


class WeatherData(models.Model):
    """
    One weather snapshot per city per timestamp.
    recorded_at is indexed heavily — range queries dominate.
    """
    city        = models.CharField(max_length=128, db_index=True)
    temperature = models.FloatField(help_text="°C")
    humidity    = models.FloatField(help_text="%")
    rainfall    = models.FloatField(default=0.0, help_text="mm in past hour")
    wind_speed  = models.FloatField(help_text="km/h")
    recorded_at = models.DateTimeField(db_index=True)

    class Meta:
        db_table = "weather_data"
        ordering = ["-recorded_at"]
        indexes = [
            models.Index(fields=["city", "recorded_at"], name="idx_weather_city_time"),
        ]

    def __str__(self):
        return f"{self.city} | {self.temperature}°C | {self.recorded_at:%Y-%m-%d %H:%M}"
