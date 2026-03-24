from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.config import PROCESSED_DIR


def load_forecast_dataframe() -> pd.DataFrame:
    forecast_path = PROCESSED_DIR / "forecast.csv"

    if not forecast_path.exists():
        return pd.DataFrame(columns=["sale_date", "forecast_units_sold"])

    df = pd.read_csv(forecast_path)

    if df.empty:
        return pd.DataFrame(columns=["sale_date", "forecast_units_sold"])

    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["forecast_units_sold"] = pd.to_numeric(df["forecast_units_sold"], errors="coerce")

    df = df.dropna(subset=["sale_date", "forecast_units_sold"]).reset_index(drop=True)
    return df