from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class SourceType(str, Enum):
    POS = "pos"
    DELIVERY = "delivery"


CANONICAL_COLUMNS: List[str] = [
    "ts",
    "sku",
    "product_name",
    "category",
    "qty",
    "unit_price_gbp",
    "line_total_gbp",
    "transaction_id",
    "source",
]

REQUIRED_CANONICAL_COLUMNS: List[str] = [
    "ts",
    "sku",
    "qty",
    "line_total_gbp",
    "source",
]

OPTIONAL_CANONICAL_COLUMNS: List[str] = [
    "product_name",
    "category",
    "unit_price_gbp",
    "transaction_id",
]

UNKNOWN_CATEGORY_LABEL = "Other"


RAW_TO_CANONICAL_COLUMN_MAP: Dict[str, Dict[str, str]] = {
    "common": {
        "timestamp": "ts",
        "date_time": "ts",
        "datetime": "ts",
        "date": "ts",
        "time_stamp": "ts",
        "sku": "sku",
        "item_sku": "sku",
        "product_sku": "sku",
        "stock_code": "sku",
        "plu": "sku",
        "product_name": "product_name",
        "item_name": "product_name",
        "product": "product_name",
        "name": "product_name",
        "category": "category",
        "department": "category",
        "group": "category",
        "qty": "qty",
        "quantity": "qty",
        "units": "qty",
        "unit_price": "unit_price_gbp",
        "price": "unit_price_gbp",
        "unit_price_gbp": "unit_price_gbp",
        "line_total": "line_total_gbp",
        "total": "line_total_gbp",
        "sales_value": "line_total_gbp",
        "revenue": "line_total_gbp",
        "amount": "line_total_gbp",
        "line_total_gbp": "line_total_gbp",
        "transaction_id": "transaction_id",
        "txn_id": "transaction_id",
        "order_id": "transaction_id",
        "receipt_id": "transaction_id",
        "source": "source",
    },
    "pos": {
        "till_time": "ts",
        "till_timestamp": "ts",
        "barcode": "sku",
        "item": "product_name",
        "dept": "category",
        "sold_qty": "qty",
        "sell_price": "unit_price_gbp",
        "gross_sales": "line_total_gbp",
        "basket_id": "transaction_id",
    },
    "delivery": {
        "order_time": "ts",
        "delivery_time": "ts",
        "menu_sku": "sku",
        "menu_item": "product_name",
        "menu_category": "category",
        "ordered_qty": "qty",
        "item_price": "unit_price_gbp",
        "order_total": "line_total_gbp",
    },
}


class CanonicalSalesRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    ts: str = Field(..., description="Raw timestamp string before parsing")
    sku: str = Field(..., min_length=1)
    qty: float
    line_total_gbp: float
    source: SourceType

    product_name: Optional[str] = None
    category: Optional[str] = None
    unit_price_gbp: Optional[float] = None
    transaction_id: Optional[str] = None


class ETLFileReadSummary(BaseModel):
    file_name: str
    source: SourceType
    detected_columns: List[str]
    mapped_columns: List[str]
    missing_required_columns: List[str]
    rows_read: int