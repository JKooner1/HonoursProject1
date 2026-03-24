from app.db.session import SessionLocal
from app.kpi.service import build_kpi_bundle
from app.utils.paths import ensure_project_dirs


def main() -> None:
    ensure_project_dirs()

    with SessionLocal() as db:
        bundle = build_kpi_bundle(db)

    print("KPI build complete")
    print(f"Daily KPI rows: {len(bundle.daily_units)}")
    print(f"Weekly KPI rows: {len(bundle.weekly_units)}")
    print(f"Monthly KPI rows: {len(bundle.monthly_units)}")
    print(f"Top products rows: {len(bundle.top_products)}")
    print(f"Top categories rows: {len(bundle.top_categories)}")
    print(f"Top sub-departments rows: {len(bundle.top_sub_departments)}")
    print(f"Daily moving average rows: {len(bundle.daily_units_ma)}")

    if not bundle.daily_units.empty:
        print("\nDaily units preview:")
        print(bundle.daily_units.head(5).to_string(index=False))

    if not bundle.top_products.empty:
        print("\nTop products preview:")
        print(bundle.top_products.head(10).to_string(index=False))

    if not bundle.top_categories.empty:
        print("\nTop categories preview:")
        print(bundle.top_categories.head(10).to_string(index=False))


if __name__ == "__main__":
    main()