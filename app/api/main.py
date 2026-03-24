from fastapi import FastAPI

from app.api.routes.forecast import router as forecast_router
from app.api.routes.kpi import router as kpi_router
from app.api.schemas import HealthResponse
from app.config import APP_NAME, APP_VERSION


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
)

app.include_router(kpi_router)
app.include_router(forecast_router)


@app.get("/", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return HealthResponse(
        status="ok",
        message="Retail Analytics API is running",
    )