from app.dashboard.app import create_dashboard
from app.utils.paths import ensure_project_dirs


if __name__ == "__main__":
    ensure_project_dirs()
    app = create_dashboard()
    app.run(
        debug=True,
        dev_tools_ui=False,
        host="127.0.0.1",
        port=8050,
    )