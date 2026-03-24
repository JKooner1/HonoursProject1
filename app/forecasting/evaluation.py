from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class ForecastEvaluationResult:
    mae: float
    rmse: float
    mape: float | None
    actual_count: int


def evaluate_forecast(
    actual_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    actual_date_col: str = "sale_date",
    actual_value_col: str = "units_sold",
    forecast_date_col: str = "sale_date",
    forecast_value_col: str = "forecast_units_sold",
) -> ForecastEvaluationResult:
    if actual_df.empty or forecast_df.empty:
        raise ValueError("Actual and forecast data must both be non-empty.")

    actual = actual_df[[actual_date_col, actual_value_col]].copy()
    forecast = forecast_df[[forecast_date_col, forecast_value_col]].copy()

    actual[actual_date_col] = pd.to_datetime(actual[actual_date_col])
    forecast[forecast_date_col] = pd.to_datetime(forecast[forecast_date_col])

    merged = actual.merge(
        forecast,
        left_on=actual_date_col,
        right_on=forecast_date_col,
        how="inner",
    )

    if merged.empty:
        raise ValueError("No overlapping dates found between actual and forecast data.")

    merged["abs_error"] = (merged[actual_value_col] - merged[forecast_value_col]).abs()
    merged["sq_error"] = (merged[actual_value_col] - merged[forecast_value_col]) ** 2

    mae = float(merged["abs_error"].mean())
    rmse = float((merged["sq_error"].mean()) ** 0.5)

    non_zero_actual = merged[merged[actual_value_col] != 0].copy()
    mape = None
    if not non_zero_actual.empty:
        non_zero_actual["ape"] = (
            (non_zero_actual[actual_value_col] - non_zero_actual[forecast_value_col]).abs()
            / non_zero_actual[actual_value_col].abs()
        ) * 100
        mape = float(non_zero_actual["ape"].mean())

    return ForecastEvaluationResult(
        mae=mae,
        rmse=rmse,
        mape=mape,
        actual_count=int(len(merged)),
    )