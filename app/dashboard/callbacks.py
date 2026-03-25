from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd
import plotly.express as px
from dash import Input, Output
from plotly import graph_objects as go
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


def _apply_clean_chart_layout(fig, height: int = 420):
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=30, r=20, t=50, b=40),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font=dict(color="#111827"),
        title=dict(font=dict(size=20)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(gridcolor="#e5e7eb", zeroline=False)
    return fig


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
            metric_card("Total Units Sold", f"{total_units:,.0f}", "Across selected filter"),
            metric_card("Average Daily Units", f"{avg_daily_units:,.1f}", "Mean daily units sold"),
            metric_card("Top Department", top_department, "Highest by units sold"),
            metric_card("Next 7-Day Forecast", f"{forecast_total:,.1f}", "Projected units total"),
        ]

        if bundle.daily_units_ma.empty:
            daily_fig = go.Figure()
            daily_fig.update_layout(title="Daily Units Sold and 7-Day Moving Average")
            daily_fig = _apply_clean_chart_layout(daily_fig)
        else:
            daily_chart_df = bundle.daily_units_ma.copy()
            daily_chart_df["sale_date"] = pd.to_datetime(daily_chart_df["sale_date"])

            daily_fig = go.Figure()
            daily_fig.add_trace(
                go.Scatter(
                    x=daily_chart_df["sale_date"],
                    y=daily_chart_df["units_sold"],
                    mode="lines+markers",
                    name="Daily Units",
                    line=dict(width=3, color="#2563eb"),
                    marker=dict(size=7),
                )
            )
            daily_fig.add_trace(
                go.Scatter(
                    x=daily_chart_df["sale_date"],
                    y=daily_chart_df["units_sold_ma_7"],
                    mode="lines",
                    name="7-Day Moving Average",
                    line=dict(width=3, color="#f97316"),
                )
            )
            daily_fig.update_layout(
                title="Daily Units Sold and 7-Day Moving Average",
                xaxis_title="Date",
                yaxis_title="Units Sold",
            )
            daily_fig = _apply_clean_chart_layout(daily_fig)

        if bundle.weekly_units.empty:
            weekly_fig = go.Figure()
            weekly_fig.update_layout(title="Weekly Units Sold")
            weekly_fig = _apply_clean_chart_layout(weekly_fig)
        else:
            weekly_chart_df = bundle.weekly_units.copy()
            weekly_chart_df["week_label"] = (
                pd.to_datetime(weekly_chart_df["week_start"]).dt.strftime("%d %b")
                + " to "
                + pd.to_datetime(weekly_chart_df["week_end"]).dt.strftime("%d %b")
            )
            weekly_fig = px.bar(
                weekly_chart_df,
                x="week_label",
                y="units_sold",
                text="units_sold",
                title="Weekly Units Sold",
                labels={"week_label": "Week", "units_sold": "Units Sold"},
            )
            weekly_fig.update_traces(marker_color="#2563eb", texttemplate="%{text:.0f}")
            weekly_fig = _apply_clean_chart_layout(weekly_fig)

        if bundle.top_products.empty:
            top_products_fig = go.Figure()
            top_products_fig.update_layout(title="Top 10 Products by Units Sold")
            top_products_fig = _apply_clean_chart_layout(top_products_fig, height=500)
        else:
            top_products_fig = px.bar(
                bundle.top_products.sort_values("units_sold", ascending=True),
                x="units_sold",
                y="product_name",
                orientation="h",
                text="units_sold",
                title="Top 10 Products by Units Sold",
                labels={"units_sold": "Units Sold", "product_name": "Product"},
            )
            top_products_fig.update_traces(marker_color="#4f46e5", texttemplate="%{text:.0f}")
            top_products_fig = _apply_clean_chart_layout(top_products_fig, height=500)

        if bundle.top_categories.empty:
            top_categories_fig = go.Figure()
            top_categories_fig.update_layout(title="Top Departments by Units Sold")
            top_categories_fig = _apply_clean_chart_layout(top_categories_fig, height=500)
        else:
            top_categories_fig = px.bar(
                bundle.top_categories.sort_values("units_sold", ascending=True),
                x="units_sold",
                y="department",
                orientation="h",
                text="units_sold",
                title="Top Departments by Units Sold",
                labels={"units_sold": "Units Sold", "department": "Department"},
            )
            top_categories_fig.update_traces(marker_color="#0ea5e9", texttemplate="%{text:.0f}")
            top_categories_fig = _apply_clean_chart_layout(top_categories_fig, height=500)

        if forecast_df.empty:
            forecast_fig = go.Figure()
            forecast_fig.update_layout(title="7-Day Forecast Units Sold")
            forecast_fig = _apply_clean_chart_layout(forecast_fig)
        else:
            forecast_df = forecast_df.copy()
            forecast_df["sale_date"] = pd.to_datetime(forecast_df["sale_date"])

            forecast_fig = go.Figure()
            forecast_fig.add_trace(
                go.Scatter(
                    x=forecast_df["sale_date"],
                    y=forecast_df["forecast_units_sold"],
                    mode="lines+markers",
                    name="Forecast Units",
                    line=dict(width=3, color="#059669"),
                    marker=dict(size=8),
                )
            )
            forecast_fig.update_layout(
                title="7-Day Forecast Units Sold",
                xaxis_title="Forecast Date",
                yaxis_title="Forecast Units Sold",
            )
            forecast_fig = _apply_clean_chart_layout(forecast_fig)

        return cards, daily_fig, weekly_fig, top_products_fig, top_categories_fig, forecast_fig