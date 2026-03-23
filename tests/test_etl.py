import pandas as pd

from app.ingestion.cleaners import clean_sales_chunk
from app.ingestion.readers import build_column_mapping, normalise_column_name
from app.ingestion.schemas import SourceType


def test_normalise_column_name() -> None:
    assert normalise_column_name("Order Time") == "order_time"
    assert normalise_column_name("Unit-Price") == "unit_price"
    assert normalise_column_name("Line/Total") == "line_total"


def test_build_column_mapping_for_pos() -> None:
    input_columns = ["Till Time", "Barcode", "Item", "Dept", "Sold Qty", "Gross Sales"]
    mapping = build_column_mapping(SourceType.POS, input_columns)

    assert mapping["Till Time"] == "ts"
    assert mapping["Barcode"] == "sku"
    assert mapping["Item"] == "product_name"
    assert mapping["Dept"] == "category"
    assert mapping["Sold Qty"] == "qty"
    assert mapping["Gross Sales"] == "line_total_gbp"


def test_build_column_mapping_for_delivery() -> None:
    input_columns = ["Order Time", "Menu SKU", "Menu Category", "Ordered Qty", "Order Total"]
    mapping = build_column_mapping(SourceType.DELIVERY, input_columns)

    assert mapping["Order Time"] == "ts"
    assert mapping["Menu SKU"] == "sku"
    assert mapping["Menu Category"] == "category"
    assert mapping["Ordered Qty"] == "qty"
    assert mapping["Order Total"] == "line_total_gbp"


def test_clean_sales_chunk_removes_invalid_rows_and_maps_unknown_category() -> None:
    df = pd.DataFrame(
        [
            {
                "ts": "2025-01-10 10:30:00",
                "sku": "A100",
                "product_name": "Milk",
                "category": "",
                "qty": "2",
                "unit_price_gbp": "1.50",
                "line_total_gbp": "3.00",
                "transaction_id": "T1",
                "source": "pos",
            },
            {
                "ts": "",
                "sku": "A101",
                "product_name": "Bread",
                "category": "Bakery",
                "qty": "1",
                "unit_price_gbp": "1.20",
                "line_total_gbp": "1.20",
                "transaction_id": "T2",
                "source": "pos",
            },
        ]
    )

    result = clean_sales_chunk(df=df, file_name="sample.csv")

    assert len(result.cleaned_df) == 1
    assert result.cleaned_df.iloc[0]["category"] == "Other"
    assert result.metrics["missing_ts"] == 1
    assert len(result.issue_rows) >= 1


def test_clean_sales_chunk_removes_duplicates_by_ts_sku_source() -> None:
    df = pd.DataFrame(
        [
            {
                "ts": "2025-01-10 10:30:00",
                "sku": "A100",
                "product_name": "Milk",
                "category": "Dairy",
                "qty": "2",
                "unit_price_gbp": "1.50",
                "line_total_gbp": "3.00",
                "transaction_id": "T1",
                "source": "pos",
            },
            {
                "ts": "2025-01-10 10:30:00",
                "sku": "A100",
                "product_name": "Milk",
                "category": "Dairy",
                "qty": "2",
                "unit_price_gbp": "1.50",
                "line_total_gbp": "3.00",
                "transaction_id": "T1_DUP",
                "source": "pos",
            },
        ]
    )

    result = clean_sales_chunk(df=df, file_name="dupes.csv")

    assert len(result.cleaned_df) == 1
    assert result.metrics["duplicate_rows_removed"] == 1


def test_clean_sales_chunk_flags_refund_sign_inconsistency() -> None:
    df = pd.DataFrame(
        [
            {
                "ts": "2025-01-10 10:30:00",
                "sku": "R100",
                "product_name": "Refund Item",
                "category": "General",
                "qty": "1",
                "unit_price_gbp": "2.50",
                "line_total_gbp": "-2.50",
                "transaction_id": "R1",
                "source": "delivery",
            }
        ]
    )

    result = clean_sales_chunk(df=df, file_name="refunds.csv")

    assert len(result.cleaned_df) == 1
    assert result.metrics["refund_inconsistency_flagged"] == 1
    assert any(
        issue["issue_type"] == "refund_sign_inconsistency"
        for issue in result.issue_rows
    )