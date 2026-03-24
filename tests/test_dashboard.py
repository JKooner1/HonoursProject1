from app.dashboard.app import create_dashboard


def test_create_dashboard() -> None:
    app = create_dashboard()
    assert app is not None
    assert app.title == "Retail Analytics Dashboard"