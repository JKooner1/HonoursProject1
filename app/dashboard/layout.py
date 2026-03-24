from __future__ import annotations

from typing import Optional

from dash import dcc, html


def build_layout(
    min_date: Optional[str],
    max_date: Optional[str],
    departments: list[str],
) -> html.Div:
    return html.Div(
        [
            html.H1(
                "Retail Analytics Dashboard",
                style={"marginBottom": "8px", "color": "#111827"},
            ),
            html.P(
                "Units-based retail analytics and short-term forecasting for weekly sales report data.",
                style={"marginTop": "0", "marginBottom": "24px", "color": "#4b5563"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("Date Range", style={"fontWeight": "bold", "marginBottom": "8px", "display": "block"}),
                            dcc.DatePickerRange(
                                id="date-range",
                                min_date_allowed=min_date,
                                max_date_allowed=max_date,
                                start_date=min_date,
                                end_date=max_date,
                                display_format="YYYY-MM-DD",
                            ),
                        ],
                        style={"flex": "1"},
                    ),
                    html.Div(
                        [
                            html.Label("Department", style={"fontWeight": "bold", "marginBottom": "8px", "display": "block"}),
                            dcc.Dropdown(
                                id="department-dropdown",
                                options=[{"label": "All", "value": "ALL"}]
                                + [{"label": dept, "value": dept} for dept in departments],
                                value="ALL",
                                clearable=False,
                            ),
                        ],
                        style={"flex": "1"},
                    ),
                ],
                style={
                    "display": "flex",
                    "gap": "16px",
                    "marginBottom": "24px",
                    "flexWrap": "wrap",
                },
            ),
            html.Div(
                id="kpi-cards",
                style={
                    "display": "flex",
                    "gap": "16px",
                    "marginBottom": "24px",
                    "flexWrap": "wrap",
                },
            ),
            html.Div(
                [
                    dcc.Graph(id="daily-units-chart"),
                    dcc.Graph(id="weekly-units-chart"),
                ],
                style={"marginBottom": "24px"},
            ),
            html.Div(
                [
                    dcc.Graph(id="top-products-chart"),
                    dcc.Graph(id="top-categories-chart"),
                ],
                style={"marginBottom": "24px"},
            ),
            html.Div(
                [
                    dcc.Graph(id="forecast-chart"),
                ]
            ),
        ],
        style={
            "maxWidth": "1400px",
            "margin": "0 auto",
            "padding": "24px",
            "backgroundColor": "#f9fafb",
            "fontFamily": "Arial, sans-serif",
        },
    )