from __future__ import annotations

from dash import Dash

from app.dashboard.callbacks import get_dashboard_filter_defaults, register_callbacks
from app.dashboard.layout import build_layout


def create_dashboard() -> Dash:
    min_date, max_date, departments = get_dashboard_filter_defaults()

    app = Dash(__name__)
    app.title = "Retail Analytics Dashboard"
    app.layout = build_layout(
        min_date=min_date,
        max_date=max_date,
        departments=departments,
    )

    register_callbacks(app)
    return app