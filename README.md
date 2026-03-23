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

- Python 3.12
- Pandas
- NumPy
- FastAPI
- Uvicorn
- Pydantic
- SQLAlchemy
- SQLite
- statsmodels
- Plotly Dash

---

## Project Structure

- `app/` → application source code  
- `data/` → raw data, processed outputs, logs  
- `db/` → SQLite database  
- `scripts/` → repeatable scripts  
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

# Create Environment
```powershell
python -m venv .venv
```

# Activate Environment
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.venv\Scripts\Activate.ps1
```

# Upgrade pip and install dependencies
```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
```

# Initialise project directories
```powershell
python scripts/init_db.py
```

# Run tests
```powershell
pytest
```

# Run API
```powershell
python run_api.py
```

Open - http://127.0.0.1:8000

# Run Dashboard
```powershell
python run_dashboard.py
```

open - http://127.0.0.1:8050