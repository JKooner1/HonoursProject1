from __future__ import annotations

from typing import Optional

from dash import dcc, html

from app.dashboard.components import section_card


def build_layout(
    min_date: Optional[str],
    max_date: Optional[str],
    departments: list[str],
) -> html.Div:
    controls = html.Div(
        [
            html.Div(
                [
                    html.Label(
                        "Date Range",
                        style={
                            "fontWeight": "600",
                            "marginBottom": "8px",
                            "display": "block",
                            "color": "#111827",
                        },
                    ),
                    dcc.DatePickerRange(
                        id="date-range",
                        min_date_allowed=min_date,
                        max_date_allowed=max_date,
                        start_date=min_date,
                        end_date=max_date,
                        display_format="YYYY-MM-DD",
                        style={"width": "100%"},
                    ),
                ],
                style={"flex": "1", "minWidth": "280px"},
            ),
            html.Div(
                [
                    html.Label(
                        "Department",
                        style={
                            "fontWeight": "600",
                            "marginBottom": "8px",
                            "display": "block",
                            "color": "#111827",
                        },
                    ),
                    dcc.Dropdown(
                        id="department-dropdown",
                        options=[{"label": "All Departments", "value": "ALL"}]
                        + [{"label": dept, "value": dept} for dept in departments],
                        value="ALL",
                        clearable=False,
                    ),
                ],
                style={"flex": "1", "minWidth": "280px"},
            ),
        ],
        style={
            "display": "flex",
            "gap": "16px",
            "flexWrap": "wrap",
        },
    )

    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(
                                "Retail Analytics Dashboard",
                                style={
                                    "fontSize": "42px",
                                    "fontWeight": "800",
                                    "color": "#111827",
                                    "marginBottom": "10px",
                                },
                            ),
                            html.Div(
                                "End-to-end retail analytics prototype using weekly sales report exports, daily KPI aggregation, and short-term forecast support.",
                                style={
                                    "fontSize": "16px",
                                    "color": "#4b5563",
                                    "maxWidth": "920px",
                                    "lineHeight": "1.6",
                                },
                            ),
                        ],
                    ),
                ],
                style={
                    "background": "linear-gradient(135deg, #ffffff 0%, #f3f4f6 100%)",
                    "border": "1px solid #e5e7eb",
                    "borderRadius": "22px",
                    "padding": "28px 30px",
                    "boxShadow": "0 10px 30px rgba(17, 24, 39, 0.06)",
                    "marginBottom": "24px",
                },
            ),
            section_card(controls, title="Filters"),
            html.Div(
                id="kpi-cards",
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(auto-fit, minmax(220px, 1fr))",
                    "gap": "16px",
                    "marginBottom": "24px",
                },
            ),
            section_card(
                [
                    dcc.Graph(
                        id="daily-units-chart",
                        config={"displaylogo": False},
                    ),
                    dcc.Graph(
                        id="weekly-units-chart",
                        config={"displaylogo": False},
                    ),
                ],
                title="Demand Overview",
            ),
            section_card(
                [
                    html.Div(
                        [
                            dcc.Graph(
                                id="top-products-chart",
                                config={"displaylogo": False},
                                style={"flex": "1", "minWidth": "420px"},
                            ),
                            dcc.Graph(
                                id="top-categories-chart",
                                config={"displaylogo": False},
                                style={"flex": "1", "minWidth": "420px"},
                            ),
                        ],
                        style={
                            "display": "flex",
                            "gap": "16px",
                            "flexWrap": "wrap",
                        },
                    )
                ],
                title="Top Sellers",
            ),
            section_card(
                [
                    dcc.Graph(
                        id="forecast-chart",
                        config={"displaylogo": False},
                    )
                ],
                title="Short-Term Forecast",
            ),
        ],
        style={
            "maxWidth": "1440px",
            "margin": "0 auto",
            "padding": "28px",
            "backgroundColor": "#f3f4f6",
            "fontFamily": "Arial, sans-serif",
            "minHeight": "100vh",
        },
    )