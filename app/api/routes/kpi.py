from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, status

from app.api.schemas import (
    DailyKPIFilters,
    DailyKPIItem,
    DailyKPIMeta,
    DailyKPIResponse,
    WeeklyKPIFilters,
    WeeklyKPIItem,
    WeeklyKPIMeta,
    WeeklyKPIResponse,
)
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
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be later than end_date.",
        )

    with SessionLocal() as db:
        bundle = build_kpi_bundle(
            db,
            start_date=start_date,
            end_date=end_date,
            department=department,
            sub_department=sub_department,
        )

    if bundle.daily_units_ma.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No daily KPI data found for the requested filters.",
        )

    items = [
        DailyKPIItem(
            sale_date=row.sale_date.date(),
            units_sold=float(row.units_sold),
            units_sold_ma_7=float(row.units_sold_ma_7)
            if hasattr(row, "units_sold_ma_7") and row.units_sold_ma_7 is not None
            else None,
        )
        for row in bundle.daily_units_ma.itertuples(index=False)
    ]

    return DailyKPIResponse(
        meta=DailyKPIMeta(
            total_items=len(items),
            metric="units_sold",
            filters=DailyKPIFilters(
                start_date=start_date,
                end_date=end_date,
                department=department,
                sub_department=sub_department,
            ),
        ),
        items=items,
    )


@router.get("/weekly", response_model=WeeklyKPIResponse)
def get_weekly_kpi(
    start_date: Optional[date] = Query(default=None),
    end_date: Optional[date] = Query(default=None),
    department: Optional[str] = Query(default=None),
    sub_department: Optional[str] = Query(default=None),
) -> WeeklyKPIResponse:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_date cannot be later than end_date.",
        )

    with SessionLocal() as db:
        bundle = build_kpi_bundle(
            db,
            start_date=start_date,
            end_date=end_date,
            department=department,
            sub_department=sub_department,
        )

    if bundle.weekly_units.empty:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No weekly KPI data found for the requested filters.",
        )

    items = [
        WeeklyKPIItem(
            week_start=row.week_start.date(),
            week_end=row.week_end.date(),
            units_sold=float(row.units_sold),
        )
        for row in bundle.weekly_units.itertuples(index=False)
    ]

    return WeeklyKPIResponse(
        meta=WeeklyKPIMeta(
            total_items=len(items),
            metric="units_sold",
            filters=WeeklyKPIFilters(
                start_date=start_date,
                end_date=end_date,
                department=department,
                sub_department=sub_department,
            ),
        ),
        items=items,
    )