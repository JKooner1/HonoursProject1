from __future__ import annotations

import pandas as pd

from app.config import PROCESSED_DIR
from app.db.session import SessionLocal
from app.forecasting.evaluation import evaluate_forecast
from app.forecasting.service import load_forecast_dataframe
from app.kpi.service import load_sales_daily_dataframe
from app.utils.paths import ensure_project_dirs


def main() -> None:
    ensure_project_dirs()

    with SessionLocal() as db:
        sales_df = load_sales_daily_dataframe(db)

    if sales_df.empty:
        print("No sales data available in the database.")
        return

    daily_actual = (
        sales_df.groupby("sale_date", as_index=False)["units_sold"]
        .sum()
        .sort_values("sale_date")
        .reset_index(drop=True)
    )

    forecast_df = load_forecast_dataframe()

    if forecast_df.empty:
        print("No forecast.csv found. Run forecasting first.")
        return

    # Naive evaluation approach:
    # compare the final available forecast window against the last available actual dates
    horizon = len(forecast_df)
    actual_tail = daily_actual.tail(horizon).copy()

    synthetic_forecast = forecast_df.copy()
    synthetic_forecast["sale_date"] = actual_tail["sale_date"].values

    result = evaluate_forecast(
        actual_df=actual_tail,
        forecast_df=synthetic_forecast,
        actual_date_col="sale_date",
        actual_value_col="units_sold",
        forecast_date_col="sale_date",
        forecast_value_col="forecast_units_sold",
    )

    output_df = pd.DataFrame(
        [
            {
                "mae": result.mae,
                "rmse": result.rmse,
                "mape": result.mape,
                "actual_count": result.actual_count,
            }
        ]
    )

    output_path = PROCESSED_DIR / "forecast_evaluation.csv"
    output_df.to_csv(output_path, index=False)

    print("Forecast evaluation complete")
    print(f"MAE: {result.mae:.2f}")
    print(f"RMSE: {result.rmse:.2f}")
    print(f"MAPE: {result.mape:.2f}%" if result.mape is not None else "MAPE: N/A")
    print(f"Rows compared: {result.actual_count}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()