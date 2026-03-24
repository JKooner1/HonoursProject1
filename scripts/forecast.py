import sqlite3
import pandas as pd
from datetime import timedelta

DB_PATH = "db/retail_analytics.db"
OUTPUT_PATH = "data/processed/forecast.csv"


def load_daily_sales():
    conn = sqlite3.connect(DB_PATH)

    query = """
    SELECT sale_date, SUM(units_sold) as units_sold
    FROM sales_daily
    GROUP BY sale_date
    ORDER BY sale_date
    """

    df = pd.read_sql(query, conn)
    conn.close()

    df["sale_date"] = pd.to_datetime(df["sale_date"])
    return df


def calculate_moving_average(df, window=7):
    df = df.copy()
    df["moving_avg"] = df["units_sold"].rolling(window=window).mean()
    return df


def forecast_next_days(df, days=7):
    last_date = df["sale_date"].max()
    last_ma = df["moving_avg"].iloc[-1]

    forecast_rows = []

    for i in range(1, days + 1):
        forecast_rows.append({
            "sale_date": last_date + timedelta(days=i),
            "forecast_units_sold": round(last_ma, 2)
        })

    forecast_df = pd.DataFrame(forecast_rows)
    return forecast_df


def run_forecast():
    print("Running forecast...")

    df = load_daily_sales()

    if len(df) < 7:
        print("Not enough data for 7-day moving average")
        return

    df = calculate_moving_average(df)

    forecast_df = forecast_next_days(df)

    forecast_df.to_csv(OUTPUT_PATH, index=False)

    print("\nForecast complete")
    print(f"Saved to: {OUTPUT_PATH}")

    print("\nForecast preview:")
    print(forecast_df.head())


if __name__ == "__main__":
    run_forecast()