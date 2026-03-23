from pathlib import Path
from typing import Generator, Iterable

import pandas as pd

from app.config import CHUNK_SIZE, RAW_DELIVERY_DIR, RAW_POS_DIR
from app.ingestion.schemas import (
    RAW_TO_CANONICAL_COLUMN_MAP,
    REQUIRED_CANONICAL_COLUMNS,
    ETLFileReadSummary,
    SourceType,
)


def normalise_column_name(column_name: str) -> str:
    return (
        str(column_name)
        .strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
    )


def build_column_mapping(source: SourceType, input_columns: Iterable[str]) -> dict[str, str]:
    common_map = RAW_TO_CANONICAL_COLUMN_MAP["common"]
    source_map = RAW_TO_CANONICAL_COLUMN_MAP[source.value]

    mapping: dict[str, str] = {}

    for original_column in input_columns:
        normalised = normalise_column_name(original_column)

        if normalised in source_map:
            mapping[original_column] = source_map[normalised]
        elif normalised in common_map:
            mapping[original_column] = common_map[normalised]

    return mapping


def get_source_directory(source: SourceType) -> Path:
    if source == SourceType.POS:
        return RAW_POS_DIR
    if source == SourceType.DELIVERY:
        return RAW_DELIVERY_DIR
    raise ValueError(f"Unsupported source: {source}")


def list_csv_files(source: SourceType) -> list[Path]:
    source_dir = get_source_directory(source)
    if not source_dir.exists():
        return []
    return sorted(source_dir.glob("*.csv"))


def read_csv_in_chunks(
    file_path: Path,
    source: SourceType,
    chunk_size: int = CHUNK_SIZE,
) -> Generator[pd.DataFrame, None, None]:
    csv_iterator = pd.read_csv(
        file_path,
        chunksize=chunk_size,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )

    for chunk in csv_iterator:
        rename_map = build_column_mapping(source=source, input_columns=chunk.columns)
        chunk = chunk.rename(columns=rename_map)

        chunk.columns = [normalise_column_name(col) for col in chunk.columns]

        if "source" not in chunk.columns:
            chunk["source"] = source.value
        else:
            chunk["source"] = chunk["source"].astype(str).str.strip().str.lower()

        yield chunk


def inspect_csv_file(file_path: Path, source: SourceType) -> ETLFileReadSummary:
    preview = pd.read_csv(
        file_path,
        nrows=5,
        dtype=str,
        keep_default_na=False,
        encoding="utf-8-sig",
    )

    detected_columns = [normalise_column_name(col) for col in preview.columns]
    rename_map = build_column_mapping(source=source, input_columns=preview.columns)
    mapped_columns = sorted(set(rename_map.values()))

    missing_required = [
        column for column in REQUIRED_CANONICAL_COLUMNS if column not in mapped_columns and column != "source"
    ]

    row_count = sum(1 for _ in pd.read_csv(file_path, chunksize=CHUNK_SIZE, dtype=str, keep_default_na=False, encoding="utf-8-sig"))

    return ETLFileReadSummary(
        file_name=file_path.name,
        source=source,
        detected_columns=detected_columns,
        mapped_columns=mapped_columns + (["source"] if "source" not in mapped_columns else []),
        missing_required_columns=missing_required,
        rows_read=row_count,
    )