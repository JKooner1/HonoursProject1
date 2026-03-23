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