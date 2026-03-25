from dash import html


def metric_card(title: str, value: str, subtitle: str = "") -> html.Div:
    return html.Div(
        [
            html.Div(
                title,
                style={
                    "fontSize": "13px",
                    "fontWeight": "600",
                    "color": "#6b7280",
                    "marginBottom": "8px",
                    "textTransform": "uppercase",
                    "letterSpacing": "0.4px",
                },
            ),
            html.Div(
                value,
                style={
                    "fontSize": "34px",
                    "fontWeight": "700",
                    "color": "#111827",
                    "lineHeight": "1.1",
                    "marginBottom": "8px",
                },
            ),
            html.Div(
                subtitle,
                style={
                    "fontSize": "12px",
                    "color": "#9ca3af",
                },
            ),
        ],
        style={
            "backgroundColor": "#ffffff",
            "border": "1px solid #e5e7eb",
            "borderRadius": "16px",
            "padding": "18px 20px",
            "boxShadow": "0 6px 18px rgba(17, 24, 39, 0.06)",
            "minWidth": "220px",
            "flex": "1",
        },
    )


def section_card(children, title: str | None = None) -> html.Div:
    content = []
    if title:
        content.append(
            html.H3(
                title,
                style={
                    "marginTop": "0",
                    "marginBottom": "18px",
                    "fontSize": "20px",
                    "fontWeight": "700",
                    "color": "#111827",
                },
            )
        )

    if isinstance(children, list):
        content.extend(children)
    else:
        content.append(children)

    return html.Div(
        content,
        style={
            "backgroundColor": "#ffffff",
            "border": "1px solid #e5e7eb",
            "borderRadius": "18px",
            "padding": "20px",
            "boxShadow": "0 6px 18px rgba(17, 24, 39, 0.05)",
            "marginBottom": "24px",
        },
    )