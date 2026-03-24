from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    message: str


class DailyKPIItem(BaseModel):
    sale_date: date
    units_sold: float
    units_sold_ma_7: Optional[float] = None


class WeeklyKPIItem(BaseModel):
    week_start: date
    week_end: date
    units_sold: float


class ForecastDailyItem(BaseModel):
    sale_date: date
    forecast_units_sold: float


class DailyKPIResponse(BaseModel):
    items: list[DailyKPIItem]


class WeeklyKPIResponse(BaseModel):
    items: list[WeeklyKPIItem]


class ForecastDailyResponse(BaseModel):
    items: list[ForecastDailyItem]