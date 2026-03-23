from dash import Dash, html


def create_dashboard() -> Dash:
    app = Dash(__name__)
    app.layout = html.Div(
        [
            html.H1("Retail Analytics Dashboard"),
            html.P("Dashboard foundation created successfully."),
        ]
    )
    return app