from datetime import date

import pandas as pd

from app.db.base import Base
from app.db.repo import insert_sales_daily_dataframe, reset_sales_daily_table
from app.db.session import SessionLocal, engine
from app.kpi.calculators import (
    daily_units_kpi,
    daily_units_with_moving_average,
    monthly_units_kpi,
    top_categories_kpi,
    top_products_kpi,
    weekly_units_kpi,
)
from app.kpi.service import build_kpi_bundle


def _sample_sales_df() -> pd.DataFrame:
    return pd.DataFrame(
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
                "sale_date": date(2026, 2, 22),
                "department": "SOFT DRINKS",
                "sub_department": "ENERGY",
                "sku": "SKU002",
                "product_name": "MONSTER",
                "units_sold": 5.0,
                "weekly_units": 15.0,
                "weekly_value_gbp": 30.0,
                "weekly_cost_gbp": 20.0,
                "weekly_profit_gbp": 10.0,
                "estimated_unit_price_gbp": 2.0,
                "estimated_daily_value_gbp": 10.0,
                "stock_on_hand": 4.0,
                "on_order": 0.0,
                "margin_pct": 33.0,
            },
            {
                "source_file": "sample.csv",
                "report_week": "202611",
                "week_start": date(2026, 2, 22),
                "week_end": date(2026, 2, 28),
                "sale_date": date(2026, 2, 23),
                "department": "SNACKS",
                "sub_department": "CRISPS",
                "sku": "SKU003",
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


def test_daily_units_kpi() -> None:
    df = _sample_sales_df()
    df["sale_date"] = pd.to_datetime(df["sale_date"])

    result = daily_units_kpi(df)

    assert len(result) == 2
    assert float(result.loc[result["sale_date"] == pd.Timestamp("2026-02-22"), "units_sold"].iloc[0]) == 15.0
    assert float(result.loc[result["sale_date"] == pd.Timestamp("2026-02-23"), "units_sold"].iloc[0]) == 8.0


def test_weekly_units_kpi() -> None:
    df = _sample_sales_df()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df["week_end"] = pd.to_datetime(df["week_end"])

    result = weekly_units_kpi(df)

    assert len(result) == 1
    assert float(result["units_sold"].iloc[0]) == 23.0


def test_monthly_units_kpi() -> None:
    df = _sample_sales_df()
    df["sale_date"] = pd.to_datetime(df["sale_date"])

    result = monthly_units_kpi(df)

    assert len(result) == 1
    assert result["year_month"].iloc[0] == "2026-02"
    assert float(result["units_sold"].iloc[0]) == 23.0


def test_top_products_kpi() -> None:
    df = _sample_sales_df()

    result = top_products_kpi(df, limit=2)

    assert len(result) == 2
    assert result.iloc[0]["product_name"] == "REDBULL"
    assert float(result.iloc[0]["units_sold"]) == 10.0


def test_top_categories_kpi() -> None:
    df = _sample_sales_df()

    result = top_categories_kpi(df, limit=2)

    assert len(result) == 2
    assert result.iloc[0]["department"] == "SOFT DRINKS"
    assert float(result.iloc[0]["units_sold"]) == 15.0


def test_daily_units_with_moving_average() -> None:
    df = _sample_sales_df()
    df["sale_date"] = pd.to_datetime(df["sale_date"])

    result = daily_units_with_moving_average(df, window=2)

    assert len(result) == 2
    assert "units_sold_ma_2" in result.columns
    assert float(result.iloc[1]["units_sold_ma_2"]) == 11.5


def test_build_kpi_bundle_from_database() -> None:
    Base.metadata.create_all(bind=engine)
    sample_df = _sample_sales_df()

    with SessionLocal() as db:
        reset_sales_daily_table(db)
        insert_sales_daily_dataframe(db, sample_df)
        db.commit()

        bundle = build_kpi_bundle(db)

        assert len(bundle.daily_units) == 2
        assert len(bundle.weekly_units) == 1
        assert len(bundle.monthly_units) == 1
        assert len(bundle.top_products) > 0
        assert len(bundle.top_categories) > 0
        assert len(bundle.daily_units_ma) == 2