from datetime import date
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from app.api.main import app
from app.config import PROCESSED_DIR
from app.db.base import Base
from app.db.repo import insert_sales_daily_dataframe, reset_sales_daily_table
from app.db.session import SessionLocal, engine

client = TestClient(app)


def _seed_sales_data() -> None:
    Base.metadata.create_all(bind=engine)

    sample_df = pd.DataFrame(
        [
            {
                "source_file": "sample.csv",
                "report_week": "202611",
                "week_start": date(2026, 2, 22),
                "week_end": date(2026, 2, 28),
                "sale_date": date(2026, 2, 22),
                "department": "SOFT DRINKS",
                "sub_department": "ENERGY",
                "sku": "SKU001",
                "product_name": "REDBULL",
                "units_sold": 10.0,
                "weekly_units": 20.0,
                "weekly_value_gbp": 50.0,
                "weekly_cost_gbp": 30.0,
                "weekly_profit_gbp": 20.0,
                "estimated_unit_price_gbp": 2.5,
                "estimated_daily_value_gbp": 25.0,
                "stock_on_hand": 5.0,
                "on_order": 1.0,
                "margin_pct": 40.0,
            },
            {
                "source_file": "sample.csv",
                "report_week": "202611",
                "week_start": date(2026, 2, 22),
                "week_end": date(2026, 2, 28),
                "sale_date": date(2026, 2, 23),
                "department": "SNACKS",
                "sub_department": "CRISPS",
                "sku": "SKU002",
                "product_name": "DORITOS",
                "units_sold": 8.0,
                "weekly_units": 8.0,
                "weekly_value_gbp": 12.0,
                "weekly_cost_gbp": 7.0,
                "weekly_profit_gbp": 5.0,
                "estimated_unit_price_gbp": 1.5,
                "estimated_daily_value_gbp": 12.0,
                "stock_on_hand": 10.0,
                "on_order": 2.0,
                "margin_pct": 41.0,
            },
        ]
    )

    with SessionLocal() as db:
        reset_sales_daily_table(db)
        insert_sales_daily_dataframe(db, sample_df)
        db.commit()


def _seed_forecast_file() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    forecast_path = PROCESSED_DIR / "forecast.csv"

    forecast_df = pd.DataFrame(
        [
            {"sale_date": "2026-03-15", "forecast_units_sold": 980.86},
            {"sale_date": "2026-03-16", "forecast_units_sold": 980.86},
        ]
    )
    forecast_df.to_csv(forecast_path, index=False)


def test_healthcheck() -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_daily_kpi_endpoint() -> None:
    _seed_sales_data()

    response = client.get("/kpi/daily")
    assert response.status_code == 200

    payload = response.json()
    assert "items" in payload
    assert len(payload["items"]) == 2
    assert payload["items"][0]["units_sold"] == 10.0


def test_weekly_kpi_endpoint() -> None:
    _seed_sales_data()

    response = client.get("/kpi/weekly")
    assert response.status_code == 200

    payload = response.json()
    assert "items" in payload
    assert len(payload["items"]) == 1
    assert payload["items"][0]["units_sold"] == 18.0


def test_forecast_daily_endpoint() -> None:
    _seed_forecast_file()

    response = client.get("/forecast/daily")
    assert response.status_code == 200

    payload = response.json()
    assert "items" in payload
    assert len(payload["items"]) == 2
    assert payload["items"][0]["forecast_units_sold"] == 980.86