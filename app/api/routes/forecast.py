from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from app.api.schemas import ForecastDailyItem, ForecastDailyMeta, ForecastDailyResponse
from app.forecasting.service import load_forecast_dataframe

router = APIRouter(prefix="/forecast", tags=["forecast"])


@router.get("/daily", response_model=ForecastDailyResponse)
def get_daily_forecast() -> ForecastDailyResponse:
    df = load_forecast_dataframe()

    if df.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No forecast data found. Run the forecasting step first.",
        )

    items = [
        ForecastDailyItem(
            sale_date=row.sale_date.date(),
            forecast_units_sold=float(row.forecast_units_sold),
        )
        for row in df.itertuples(index=False)
    ]

    return ForecastDailyResponse(
        meta=ForecastDailyMeta(
            forecast_horizon_days=len(items),
            source="data/processed/forecast.csv",
        ),
        items=items,
    )