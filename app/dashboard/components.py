from dash import html


def metric_card(title: str, value: str, subtitle: str = "") -> html.Div:
    return html.Div(
        [
            html.Div(title, style={"fontSize": "14px", "color": "#666", "marginBottom": "6px"}),
            html.Div(value, style={"fontSize": "28px", "fontWeight": "bold", "color": "#111"}),
            html.Div(subtitle, style={"fontSize": "12px", "color": "#888", "marginTop": "6px"}),
        ],
        style={
            "backgroundColor": "#ffffff",
            "border": "1px solid #e5e7eb",
            "borderRadius": "12px",
            "padding": "16px",
            "boxShadow": "0 1px 3px rgba(0,0,0,0.08)",
            "minWidth": "220px",
            "flex": "1",
        },
    )