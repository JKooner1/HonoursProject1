from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd


def apply_date_filter(
    df: pd.DataFrame,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    working_df = df.copy()

    if start_date is not None:
        working_df = working_df[working_df["sale_date"] >= pd.Timestamp(start_date)]

    if end_date is not None:
        working_df = working_df[working_df["sale_date"] <= pd.Timestamp(end_date)]

    return working_df.reset_index(drop=True)


def apply_category_filter(
    df: pd.DataFrame,
    department: Optional[str] = None,
    sub_department: Optional[str] = None,
) -> pd.DataFrame:
    working_df = df.copy()

    if department:
        working_df = working_df[working_df["department"] == department]

    if sub_department:
        working_df = working_df[working_df["sub_department"] == sub_department]

    return working_df.reset_index(drop=True)


def daily_units_kpi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sale_date", "units_sold"])

    result = (
        df.groupby("sale_date", as_index=False)["units_sold"]
        .sum()
        .sort_values("sale_date")
        .reset_index(drop=True)
    )
    return result


def weekly_units_kpi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["week_start", "week_end", "units_sold"])

    result = (
        df.groupby(["week_start", "week_end"], as_index=False)["units_sold"]
        .sum()
        .sort_values(["week_start", "week_end"])
        .reset_index(drop=True)
    )
    return result


def monthly_units_kpi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["year_month", "units_sold"])

    working_df = df.copy()
    working_df["year_month"] = pd.to_datetime(working_df["sale_date"]).dt.to_period("M").astype(str)

    result = (
        working_df.groupby("year_month", as_index=False)["units_sold"]
        .sum()
        .sort_values("year_month")
        .reset_index(drop=True)
    )
    return result


def top_products_kpi(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["product_name", "units_sold"])

    result = (
        df.groupby("product_name", as_index=False)["units_sold"]
        .sum()
        .sort_values(["units_sold", "product_name"], ascending=[False, True])
        .head(limit)
        .reset_index(drop=True)
    )
    return result


def top_categories_kpi(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["department", "units_sold"])

    result = (
        df.groupby("department", as_index=False)["units_sold"]
        .sum()
        .sort_values(["units_sold", "department"], ascending=[False, True])
        .head(limit)
        .reset_index(drop=True)
    )
    return result


def sub_department_units_kpi(df: pd.DataFrame, limit: int = 10) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["sub_department", "units_sold"])

    result = (
        df.groupby("sub_department", as_index=False)["units_sold"]
        .sum()
        .sort_values(["units_sold", "sub_department"], ascending=[False, True])
        .head(limit)
        .reset_index(drop=True)
    )
    return result


def daily_units_with_moving_average(df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    daily_df = daily_units_kpi(df)

    if daily_df.empty:
        return pd.DataFrame(columns=["sale_date", "units_sold", "units_sold_ma_7"])

    daily_df = daily_df.sort_values("sale_date").reset_index(drop=True)
    daily_df[f"units_sold_ma_{window}"] = (
        daily_df["units_sold"].rolling(window=window, min_periods=1).mean()
    )
    return daily_df