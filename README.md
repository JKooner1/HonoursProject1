# Retail Analytics Dashboard

A clean end-to-end retail analytics system for a UK convenience store.

The system will:
- ingest raw sales CSV files
- perform ETL and data quality validation
- store cleaned and aggregated data in SQLite
- generate KPIs and short-term forecasts
- expose results through a FastAPI backend
- visualise outputs in a Plotly Dash dashboard

## Tech Stack

- Python 3.11
- Pandas
- NumPy
- FastAPI
- Uvicorn
- Pydantic
- SQLAlchemy
- SQLite
- statsmodels
- Plotly Dash

## Project Structure

- `app/` application source code
- `data/` raw data, processed outputs, logs
- `db/` SQLite database
- `scripts/` repeatable script entry points
- `tests/` test suite

## Notes

- No PII
- Revenue is VAT-inclusive
- Refunds remain negative
- Duplicate transactions must be removed
- Missing timestamps must be handled or logged
- `.venv` must never be committed