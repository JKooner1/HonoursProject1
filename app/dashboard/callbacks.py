from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd
import plotly.express as px
from dash import Input, Output, html
from sqlalchemy import select

from app.dashboard.components import metric_card
from app.db.models import SalesDaily
from app.db.session import SessionLocal
from app.forecasting.service import load_forecast_dataframe
from app.kpi.service import build_kpi_bundle, load_sales_daily_dataframe


def _get_department_list() -> list[str]:
    with SessionLocal() as db:
        departments = db.execute(
            select(SalesDaily.department).distinct().order_by(SalesDaily.department.asc())
        ).scalars().all()
    return [dept for dept in departments if dept]


def get_dashboard_filter_defaults() -> tuple[Optional[str], Optional[str], list[str]]:
    with SessionLocal() as db:
        df = load_sales_daily_dataframe(db)

    if df.empty:
        return None, None, []

    min_date = df["sale_date"].min().date().isoformat()
    max_date = df["sale_date"].max().date().isoformat()
    departments = _get_department_list()
    return min_date, max_date, departments


def register_callbacks(app) -> None:
    @app.callback(
        Output("kpi-cards", "children"),
        Output("daily-units-chart", "figure"),
        Output("weekly-units-chart", "figure"),
        Output("top-products-chart", "figure"),
        Output("top-categories-chart", "figure"),
        Output("forecast-chart", "figure"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),
        Input("department-dropdown", "value"),
    )
    def update_dashboard(
        start_date: Optional[str],
        end_date: Optional[str],
        department_value: str,
    ):
        department_filter = None if department_value in (None, "ALL") else department_value

        start_date_obj = date.fromisoformat(start_date) if start_date else None
        end_date_obj = date.fromisoformat(end_date) if end_date else None

        with SessionLocal() as db:
            bundle = build_kpi_bundle(
                db,
                start_date=start_date_obj,
                end_date=end_date_obj,
                department=department_filter,
                sub_department=None,
            )
            base_df = load_sales_daily_dataframe(db)

        if department_filter:
            base_df = base_df[base_df["department"] == department_filter].reset_index(drop=True)

        if start_date_obj:
            base_df = base_df[base_df["sale_date"] >= pd.Timestamp(start_date_obj)]

        if end_date_obj:
            base_df = base_df[base_df["sale_date"] <= pd.Timestamp(end_date_obj)]

        total_units = float(base_df["units_sold"].sum()) if not base_df.empty else 0.0
        avg_daily_units = float(bundle.daily_units["units_sold"].mean()) if not bundle.daily_units.empty else 0.0

        top_department = "N/A"
        if not bundle.top_categories.empty:
            top_department = str(bundle.top_categories.iloc[0]["department"])

        forecast_df = load_forecast_dataframe()
        forecast_total = float(forecast_df["forecast_units_sold"].sum()) if not forecast_df.empty else 0.0

        cards = [
            metric_card("Total Units Sold", f"{total_units:,.0f}", "Filtered selection"),
            metric_card("Average Daily Units", f"{avg_daily_units:,.1f}", "Across selected date range"),
            metric_card("Top Department", top_department, "By units sold"),
            metric_card("Next 7-Day Forecast", f"{forecast_total:,.1f}", "Forecast units total"),
        ]

        if bundle.daily_units_ma.empty:
            daily_fig = px.line(title="Daily Units Sold")
        else:
            daily_chart_df = bundle.daily_units_ma.copy()
            daily_chart_df["sale_date"] = pd.to_datetime(daily_chart_df["sale_date"])
            daily_fig = px.line(
                daily_chart_df,
                x="sale_date",
                y=["units_sold", "units_sold_ma_7"],
                title="Daily Units Sold and 7-Day Moving Average",
                labels={"value": "Units Sold", "sale_date": "Date", "variable": "Series"},
            )
            daily_fig.update_layout(template="plotly_white")

        if bundle.weekly_units.empty:
            weekly_fig = px.bar(title="Weekly Units Sold")
        else:
            weekly_chart_df = bundle.weekly_units.copy()
            weekly_chart_df["week_label"] = (
                pd.to_datetime(weekly_chart_df["week_start"]).dt.strftime("%Y-%m-%d")
                + " to "
                + pd.to_datetime(weekly_chart_df["week_end"]).dt.strftime("%Y-%m-%d")
            )
            weekly_fig = px.bar(
                weekly_chart_df,
                x="week_label",
                y="units_sold",
                title="Weekly Units Sold",
                labels={"week_label": "Week", "units_sold": "Units Sold"},
            )
            weekly_fig.update_layout(template="plotly_white")

        if bundle.top_products.empty:
            top_products_fig = px.bar(title="Top Products")
        else:
            top_products_fig = px.bar(
                bundle.top_products.sort_values("units_sold", ascending=True),
                x="units_sold",
                y="product_name",
                orientation="h",
                title="Top 10 Products by Units Sold",
                labels={"units_sold": "Units Sold", "product_name": "Product"},
            )
            top_products_fig.update_layout(template="plotly_white")

        if bundle.top_categories.empty:
            top_categories_fig = px.bar(title="Top Categories")
        else:
            top_categories_fig = px.bar(
                bundle.top_categories.sort_values("units_sold", ascending=True),
                x="units_sold",
                y="department",
                orientation="h",
                title="Top Departments by Units Sold",
                labels={"units_sold": "Units Sold", "department": "Department"},
            )
            top_categories_fig.update_layout(template="plotly_white")

        if forecast_df.empty:
            forecast_fig = px.line(title="7-Day Forecast")
        else:
            forecast_df = forecast_df.copy()
            forecast_df["sale_date"] = pd.to_datetime(forecast_df["sale_date"])
            forecast_fig = px.line(
                forecast_df,
                x="sale_date",
                y="forecast_units_sold",
                markers=True,
                title="7-Day Forecast Units Sold",
                labels={"sale_date": "Date", "forecast_units_sold": "Forecast Units Sold"},
            )
            forecast_fig.update_layout(template="plotly_white")

        return cards, daily_fig, weekly_fig, top_products_fig, top_categories_fig, forecast_fig