from datetime import date

import pandas as pd

from app.forecasting.evaluation import evaluate_forecast
from app.forecasting.service import load_forecast_dataframe


def test_evaluate_forecast_returns_metrics() -> None:
    actual_df = pd.DataFrame(
        [
            {"sale_date": date(2026, 3, 1), "units_sold": 100.0},
            {"sale_date": date(2026, 3, 2), "units_sold": 120.0},
        ]
    )

    forecast_df = pd.DataFrame(
        [
            {"sale_date": date(2026, 3, 1), "forecast_units_sold": 110.0},
            {"sale_date": date(2026, 3, 2), "forecast_units_sold": 115.0},
        ]
    )

    result = evaluate_forecast(actual_df, forecast_df)

    assert result.actual_count == 2
    assert round(result.mae, 2) == 7.50
    assert round(result.rmse, 2) == 7.91
    assert result.mape is not None


def test_load_forecast_dataframe_handles_missing_file(tmp_path, monkeypatch) -> None:
    from app import forecasting
    from app.forecasting import service

    monkeypatch.setattr(service, "PROCESSED_DIR", tmp_path)

    df = load_forecast_dataframe()

    assert df.empty
    assert list(df.columns) == ["sale_date", "forecast_units_sold"]