# Datastore Flask Skeleton

Lightweight Flask app for authentication/admin plus a report-builder sync that mirrors the SQL Server view `rpt.RGRitten` into a local SQLite table `rgritten`.

## Purpose & Scope
- Manage users/roles and connection profiles to remote SQL Server instances.
- Pull data from the `Historie` profile’s view `rpt.RGRitten` into local SQLite for reporting.
- Append-only ingest keyed by `(ritdatum, ritnummer)`; supports date filtering; currently forward-only (no backfill toggle yet).

## Architecture
- Flask blueprints: `auth`, `admin`, `main` (routes in `blueprints/*/routes.py`).
- Data layer: SQLAlchemy via Flask-SQLAlchemy; migrations via Flask-Migrate.
- Local dev DB: SQLite in `instance/app.db`.
- Remote fetch: direct SQLAlchemy engine to SQL Server via `pyodbc` using saved `ConnectionProfile`.
- Sync helper: `rgritten_sync.py` with CLI commands in `app.py`.
- Report builder: blueprint `reports` with templates persisted in `report_templates`; dataset `rgritten` selectable with per-field include/filter/group/sort, run view, CSV/XLSX export.

## Stack (versions in this env)
- Python 3.13 (venv: `.venv`)
- Flask 3.1.0
- Flask-Login 0.6.3
- Flask-WTF 1.2.2
- email-validator 2.2.0
- Flask-Migrate 4.1.0
- Flask-SQLAlchemy 3.1.1
- SQLAlchemy 2.0.40
- python-dotenv 1.1.0
- passlib 1.7.4
- pyodbc 5.2.0
- pytest 8.4.2
- gunicorn/waitress: listed in requirements but not installed in this env.

## Setup & Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# run app (dev)
flask --app app:create_app run
# seed roles/admin (dev only)
flask --app app:create_app run &  # or python app.py
open http://localhost:5000/init   # guarded by INIT_TOKEN outside debug
```

## Database & Migrations
```bash
flask db init   # once
flask db migrate -m "msg"
flask db upgrade
```
Local DB file: `instance/app.db`.

## RGRitten Sync (report ingest)
- Local model: `models.RGRit` table `rgritten` (no unique constraint on owner/ritnummer).
- Cursor: latest `(ritdatum, ritnummer)` stored; forward-only.
- CLI:
```bash
flask sync-rgritten --profile Historie [--chunk-size 1000] [--min-ritdatum YYYY-MM-DD]
flask diagnose-rgritten --profile Historie [--cursor 0] [--min-ritdatum YYYY-MM-DD]
flask debug-rgritten-cols --profile Historie
```
- Remote SELECT uses `TRY_CONVERT` casts; Decimals cast to float before SQLite insert.

## Recent Decisions / Changelog-lite
- Added `RGRit` model and append-only sync pipeline (`rgritten_sync.py` + CLI commands).
- Added report builder (`/reports`) with template creation and run/export.
- Removed unique constraint `(owner_id, ritnummer)` to allow repeated trip numbers; rebuilt SQLite table to drop the constraint.
- Sync cursor now uses `(ritdatum, ritnummer)` ordering and supports `--min-ritdatum`; no backfill toggle yet.
- Decimal → float casting to satisfy SQLite binding.
- Diagnostics: column isolator (`debug-rgritten-cols`) and conversion checker (`diagnose-rgritten`).
- Outstanding: SQL Server login timeout for profile `Historie`; backfill flag to ingest earlier dates without clearing DB; pytest currently failing due to import path (`ModuleNotFoundError: app`).

## Notes
- Ensure `Historie` connection profile is populated (host/port/db/user/pass/driver).
- Current run error: login timeout to SQL Server; address network/credentials before re-running sync.
- Tests: `pytest -q` currently fails in fixture import; needs path/package fix.
