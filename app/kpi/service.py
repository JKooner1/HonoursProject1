from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional

import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import SalesDaily
from app.kpi.calculators import (
    apply_category_filter,
    apply_date_filter,
    daily_units_kpi,
    daily_units_with_moving_average,
    monthly_units_kpi,
    sub_department_units_kpi,
    top_categories_kpi,
    top_products_kpi,
    weekly_units_kpi,
)


@dataclass
class KPIBundle:
    daily_units: pd.DataFrame
    weekly_units: pd.DataFrame
    monthly_units: pd.DataFrame
    top_products: pd.DataFrame
    top_categories: pd.DataFrame
    top_sub_departments: pd.DataFrame
    daily_units_ma: pd.DataFrame


def load_sales_daily_dataframe(db: Session) -> pd.DataFrame:
    rows = db.scalars(select(SalesDaily)).all()

    if not rows:
        return pd.DataFrame(
            columns=[
                "sale_date",
                "week_start",
                "week_end",
                "department",
                "sub_department",
                "product_name",
                "units_sold",
            ]
        )

    data = [
        {
            "sale_date": row.sale_date,
            "week_start": row.week_start,
            "week_end": row.week_end,
            "department": row.department,
            "sub_department": row.sub_department,
            "product_name": row.product_name,
            "units_sold": row.units_sold,
        }
        for row in rows
    ]

    df = pd.DataFrame(data)
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["week_start"] = pd.to_datetime(df["week_start"])
    df["week_end"] = pd.to_datetime(df["week_end"])
    return df


def build_kpi_bundle(
    db: Session,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    department: Optional[str] = None,
    sub_department: Optional[str] = None,
) -> KPIBundle:
    df = load_sales_daily_dataframe(db)
    df = apply_date_filter(df, start_date=start_date, end_date=end_date)
    df = apply_category_filter(df, department=department, sub_department=sub_department)

    return KPIBundle(
        daily_units=daily_units_kpi(df),
        weekly_units=weekly_units_kpi(df),
        monthly_units=monthly_units_kpi(df),
        top_products=top_products_kpi(df),
        top_categories=top_categories_kpi(df),
        top_sub_departments=sub_department_units_kpi(df),
        daily_units_ma=daily_units_with_moving_average(df, window=7),
    )