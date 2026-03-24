from __future__ import annotations

from datetime import date
from typing import Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str
    message: str
    app_name: str
    version: str


class APIErrorResponse(BaseModel):
    detail: str


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


class DailyKPIFilters(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    department: Optional[str] = None
    sub_department: Optional[str] = None


class WeeklyKPIFilters(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    department: Optional[str] = None
    sub_department: Optional[str] = None


class ForecastDailyMeta(BaseModel):
    forecast_horizon_days: int = Field(..., ge=0)
    source: str


class DailyKPIMeta(BaseModel):
    total_items: int = Field(..., ge=0)
    metric: str
    filters: DailyKPIFilters


class WeeklyKPIMeta(BaseModel):
    total_items: int = Field(..., ge=0)
    metric: str
    filters: WeeklyKPIFilters


class DailyKPIResponse(BaseModel):
    meta: DailyKPIMeta
    items: list[DailyKPIItem]


class WeeklyKPIResponse(BaseModel):
    meta: WeeklyKPIMeta
    items: list[WeeklyKPIItem]


class ForecastDailyResponse(BaseModel):
    meta: ForecastDailyMeta
    items: list[ForecastDailyItem]