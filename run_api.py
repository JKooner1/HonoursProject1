import uvicorn

from app.utils.paths import ensure_project_dirs


if __name__ == "__main__":
    ensure_project_dirs()
    uvicorn.run(
        "app.api.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True,
    )