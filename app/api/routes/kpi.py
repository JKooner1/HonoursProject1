from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Query

from app.api.schemas import DailyKPIItem, DailyKPIResponse, WeeklyKPIItem, WeeklyKPIResponse
from app.db.session import SessionLocal
from app.kpi.service import build_kpi_bundle

router = APIRouter(prefix="/kpi", tags=["kpi"])


@router.get("/daily", response_model=DailyKPIResponse)
def get_daily_kpi(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    department: Optional[str] = Query(default=None),
    sub_department: Optional[str] = Query(default=None),
) -> DailyKPIResponse:
    with SessionLocal() as db:
        bundle = build_kpi_bundle(
            db,
            start_date=start_date,
            end_date=end_date,
            department=department,
            sub_department=sub_department,
        )

    items = [
        DailyKPIItem(
            sale_date=row.sale_date.date(),
            units_sold=float(row.units_sold),
            units_sold_ma_7=(
                float(row.units_sold_ma_7) if "units_sold_ma_7" in bundle.daily_units_ma.columns else None
            ),
        )
        for row in bundle.daily_units_ma.itertuples(index=False)
    ]

    return DailyKPIResponse(items=items)


@router.get("/weekly", response_model=WeeklyKPIResponse)
def get_weekly_kpi(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    department: Optional[str] = Query(default=None),
    sub_department: Optional[str] = Query(default=None),
) -> WeeklyKPIResponse:
    with SessionLocal() as db:
        bundle = build_kpi_bundle(
            db,
            start_date=start_date,
            end_date=end_date,
            department=department,
            sub_department=sub_department,
        )

    items = [
        WeeklyKPIItem(
            week_start=row.week_start.date(),
            week_end=row.week_end.date(),
            units_sold=float(row.units_sold),
        )
        for row in bundle.weekly_units.itertuples(index=False)
    ]

    return WeeklyKPIResponse(items=items)