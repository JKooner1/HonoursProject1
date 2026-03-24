# Retail Analytics Dashboard

A clean end-to-end retail analytics system for a UK convenience store.

The system will:
- ingest weekly sales CSV reports
- transform them into daily transactional data
- perform ETL and data quality validation
- store cleaned data in SQLite
- generate KPIs (units-based)
- produce short-term forecasts using moving averages
- expose results via a FastAPI backend
- visualise outputs in a Plotly Dash dashboard

---

## Tech Stack

- Python 3.12
- Pandas
- NumPy
- FastAPI
- Uvicorn
- Pydantic
- SQLAlchemy
- SQLite
- Plotly Dash

---

## Project Structure

- `app/` → application source code  
- `data/raw/reports/` → weekly sales CSVs  
- `data/processed/` → transformed outputs + forecasts  
- `db/` → SQLite database  
- `scripts/` → ETL, DB, KPI, forecasting scripts  
- `tests/` → test suite  

---

## Project Rules

- No PII  
- Revenue is VAT-inclusive  
- Refunds remain negative  
- Duplicate transactions must be removed  
- Missing timestamps must be handled or logged  
- `.venv` must never be committed  

---

# Windows Setup Guide (Copy-Paste Friendly)

Run everything from inside the project folder:

```powershell
cd path\to\HonoursProject1
```

# 1. Create Virtual Environment - Creates an isolated Python environment.
python -m venv .venv

# 2. Activate Environment - Activates the environment
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1

# 3. Install Dependencies - Installs all required libraries
python -m pip install --upgrade pip
pip install -r requirements.txt

# 4. Initialise Database - Creates the SQLite database and tables.
python -m scripts.init_db

# 5. Add Raw Data
Place weekly CSV files into:
data/raw/reports/

# 6. Run ETL Pipeline
python -m scripts.run_etl

What it does:

reads weekly reports
transforms into daily-level data
validates and cleans data
stores results in SQLite

# 7. Run Tests
pytest

Validates:

ETL
database
KPI logic
forecasting

# 8. Generate KPIs
python -m scripts.build_kpis

Generates:

daily totals
weekly totals
monthly totals
top products
top categories
moving averages

# 9. Run Forecasting
python -m scripts.forecast

What it does:

calculates 7-day moving average
predicts next 7 days
saves to:
data/processed/forecast.csv

# 10. Run API
python run_api.py

Open - http://127.0.0.1:8000

# 11. Run Dashboard
python run_dashboard.py

Open - http://127.0.0.1:8050

# Full Pipeline from Scratch
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python -m scripts.init_db
python -m scripts.run_etl
python -m scripts.build_kpis
python -m scripts.forecast