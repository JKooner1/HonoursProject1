from __future__ import annotations

import pandas as pd
from sqlalchemy import select

from app.config import PROCESSED_DIR
from app.db.models import ETLRunLog, SalesDaily
from app.db.session import SessionLocal
from app.forecasting.service import load_forecast_dataframe
from app.kpi.service import build_kpi_bundle
from app.utils.paths import ensure_project_dirs


def main() -> None:
    ensure_project_dirs()

    with SessionLocal() as db:
        sales_count = len(db.scalars(select(SalesDaily)).all())
        latest_run = db.scalars(
            select(ETLRunLog).order_by(ETLRunLog.run_timestamp.desc())
        ).first()
        bundle = build_kpi_bundle(db)

    forecast_df = load_forecast_dataframe()

    summary = {
        "sales_daily_rows": sales_count,
        "daily_kpi_rows": len(bundle.daily_units),
        "weekly_kpi_rows": len(bundle.weekly_units),
        "monthly_kpi_rows": len(bundle.monthly_units),
        "top_products_rows": len(bundle.top_products),
        "top_categories_rows": len(bundle.top_categories),
        "forecast_rows": len(forecast_df),
        "latest_etl_status": latest_run.status if latest_run else None,
        "latest_etl_files_processed": latest_run.files_processed if latest_run else None,
        "latest_etl_rows_out": latest_run.rows_out if latest_run else None,
        "latest_etl_issue_count": latest_run.issue_count if latest_run else None,
    }

    summary_df = pd.DataFrame([summary])
    output_path = PROCESSED_DIR / "project_summary.csv"
    summary_df.to_csv(output_path, index=False)

    print("Project summary generated")
    for key, value in summary.items():
        print(f"{key}: {value}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()