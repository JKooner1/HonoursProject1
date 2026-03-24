import csv
from pathlib import Path

from app.ingestion.report_transformer import transform_weekly_report_file


def _write_row(writer: csv.writer, cells: dict[int, str], width: int = 64) -> None:
    row = [""] * width
    for index, value in cells.items():
        row[index] = value
    writer.writerow(row)


def _build_sample_report_csv(file_path: Path) -> None:
    with open(file_path, "w", encoding="utf-8-sig", newline="") as file_handle:
        writer = csv.writer(file_handle)

        _write_row(writer, {})
        _write_row(writer, {1: "Daily Product Sales Report"})
        _write_row(
            writer,
            {
                1: (
                    "For Week 202611    22-Feb-2026 to 28-Feb-2026\n"
                    "Suppliers: ALL\nDepartments: ALL\nSubDepartments: ALL\nBranches: PREMIER\nEvents: ALL"
                )
            },
        )
        for _ in range(6):
            _write_row(writer, {})

        _write_row(
            writer,
            {
                0: "Stk Code",
                8: "Product Description",
                19: "SUN",
                23: "MON",
                26: "TUE",
                28: "WED",
                31: "THU",
                34: "FRI",
                38: "SAT",
                41: "Total",
                46: "Value",
                50: "Cost",
                53: "Profit",
                56: "In Stk",
                59: "On Ord",
                62: "%",
            },
        )

        _write_row(writer, {})
        _write_row(writer, {0: "Dept:", 4: "2-GROCERY VATABLE"})
        _write_row(writer, {})
        _write_row(writer, {5: "Sub Dept:", 12: "3-SOFT DRINKS"})
        _write_row(writer, {})

        _write_row(
            writer,
            {
                0: "SKU001",
                8: "REDBULL 355ML",
                19: "1",
                23: "2",
                26: "3",
                28: "4",
                31: "5",
                33: "6",
                37: "7",
                42: "28",
                46: "56.00",
                50: "40.00",
                53: "16.00",
                56: "10",
                59: "0",
                61: "28.57",
            },
        )

        _write_row(
            writer,
            {
                0: "SKU002",
                8: "LUCOZADE ENERGY ORANGE",
                16: "500ML",
                19: "2",
                23: "2",
                26: "2",
                28: "2",
                31: "2",
                33: "2",
                37: "2",
                42: "20",
                46: "30.00",
                50: "18.00",
                53: "12.00",
                56: "5",
                59: "1",
                61: "40.00",
            },
        )

        _write_row(
            writer,
            {
                0: "Grand Total:",
                19: "3",
                23: "4",
            },
        )


def test_transform_weekly_report_file_outputs_daily_rows(tmp_path: Path) -> None:
    file_path = tmp_path / "sample_week.csv"
    _build_sample_report_csv(file_path)

    result = transform_weekly_report_file(file_path)

    assert len(result.transformed_df) == 14
    assert result.metrics["product_rows_detected"] == 2
    assert result.metrics["daily_rows_output"] == 14
    assert result.metrics["issue_count"] == 1


def test_transform_weekly_report_preserves_department_context(tmp_path: Path) -> None:
    file_path = tmp_path / "sample_week.csv"
    _build_sample_report_csv(file_path)

    result = transform_weekly_report_file(file_path)

    assert set(result.transformed_df["department"]) == {"GROCERY VATABLE"}
    assert set(result.transformed_df["sub_department"]) == {"SOFT DRINKS"}


def test_transform_weekly_report_estimates_daily_value_from_weekly_value(tmp_path: Path) -> None:
    file_path = tmp_path / "sample_week.csv"
    _build_sample_report_csv(file_path)

    result = transform_weekly_report_file(file_path)

    redbull_rows = result.transformed_df[
        result.transformed_df["product_name"] == "REDBULL 355ML"
    ].copy()

    assert round(redbull_rows["estimated_daily_value_gbp"].sum(), 2) == 56.00
    assert round(redbull_rows["estimated_unit_price_gbp"].iloc[0], 2) == 2.00


def test_transform_weekly_report_combines_product_name_and_variant(tmp_path: Path) -> None:
    file_path = tmp_path / "sample_week.csv"
    _build_sample_report_csv(file_path)

    result = transform_weekly_report_file(file_path)

    assert "LUCOZADE ENERGY ORANGE 500ML" in set(result.transformed_df["product_name"])