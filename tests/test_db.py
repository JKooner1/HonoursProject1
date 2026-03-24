from datetime import date

import pandas as pd

from app.db.base import Base
from app.db.repo import (
    get_latest_etl_runlog,
    get_sales_daily_count,
    insert_etl_runlog,
    insert_sales_daily_dataframe,
    reset_sales_daily_table,
)
from app.db.session import SessionLocal, engine


def test_database_tables_can_be_created() -> None:
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as db:
        assert db is not None


def test_insert_sales_daily_dataframe() -> None:
    Base.metadata.create_all(bind=engine)

    sample_df = pd.DataFrame(
        [
            {
                "source_file": "sample.csv",
                "report_week": "202611",
                "week_start": date(2026, 2, 22),
                "week_end": date(2026, 2, 28),
                "sale_date": date(2026, 2, 22),
                "department": "GROCERY VATABLE",
                "sub_department": "SOFT DRINKS",
                "sku": "SKU001",
                "product_name": "REDBULL 355ML",
                "units_sold": 1.0,
                "weekly_units": 28.0,
                "weekly_value_gbp": 56.0,
                "weekly_cost_gbp": 40.0,
                "weekly_profit_gbp": 16.0,
                "estimated_unit_price_gbp": 2.0,
                "estimated_daily_value_gbp": 2.0,
                "stock_on_hand": 10.0,
                "on_order": 0.0,
                "margin_pct": 28.57,
            }
        ]
    )

    with SessionLocal() as db:
        reset_sales_daily_table(db)
        inserted = insert_sales_daily_dataframe(db, sample_df)
        db.commit()

        assert inserted == 1
        assert get_sales_daily_count(db) == 1


def test_insert_etl_runlog() -> None:
    Base.metadata.create_all(bind=engine)

    with SessionLocal() as db:
        insert_etl_runlog(
            db,
            files_processed=1,
            rows_out=100,
            issue_count=2,
            status="success",
            notes="Test run",
        )
        db.commit()

        latest = get_latest_etl_runlog(db)

        assert latest is not None
        assert latest.files_processed == 1
        assert latest.rows_out == 100
        assert latest.issue_count == 2
        assert latest.status == "success"