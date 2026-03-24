from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.api.routes.forecast import router as forecast_router
from app.api.routes.kpi import router as kpi_router
from app.api.schemas import APIErrorResponse, HealthResponse
from app.config import APP_NAME, APP_VERSION


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="Retail analytics API for KPI retrieval and short-term forecast output.",
)

app.include_router(kpi_router)
app.include_router(forecast_router)


@app.get("/", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return HealthResponse(
        status="ok",
        message="Retail Analytics API is running",
        app_name=APP_NAME,
        version=APP_VERSION,
    )


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        message="Retail Analytics API is healthy",
        app_name=APP_NAME,
        version=APP_VERSION,
    )