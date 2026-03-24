from __future__ import annotations

from fastapi import APIRouter

from app.api.schemas import ForecastDailyItem, ForecastDailyResponse
from app.forecasting.service import load_forecast_dataframe

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.get("/daily", response_model=ForecastDailyResponse)
def get_daily_forecast() -> ForecastDailyResponse:
    df = load_forecast_dataframe()

    items = [
        ForecastDailyItem(
            sale_date=row.sale_date.date(),
            forecast_units_sold=float(row.forecast_units_sold),
        )
        for row in df.itertuples(index=False)
    ]

    return ForecastDailyResponse(items=items)