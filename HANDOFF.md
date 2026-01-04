# Handoff Summary

For the next engineer picking this up (fresh context).

## Current Status
- Flask app skeleton in place (auth/admin/main). Local DB is SQLite `instance/app.db`.
- Added `rgritten` table (`models.RGRit`) and sync pipeline in `rgritten_sync.py`.
- Sync cursor: uses latest `(ritdatum, ritnummer)` already stored; supports `--min-ritdatum`; forward-only (no backfill override yet).
- Removed unique constraint on `(owner_id, ritnummer)` and rebuilt the SQLite table accordingly.
- Decimal values cast to float before SQLite insert.
- Report builder added: blueprint `reports` with template creation/list/run stored in `report_templates`; dataset `rgritten` only; per-field include/filter/group/sort; run view supports CSV/XLSX exports (openpyxl required for xlsx).
- Latest run attempting `flask sync-rgritten --profile Historie --min-ritdatum 2025-11-10` failed due to SQL Server login timeout (HYT00).
- Prior UNIQUE errors resolved by rebuilding the table to drop the constraint.

## Open Challenges / Next Steps
1) Fix SQL Server connectivity for profile `Historie` (check host/port/user/pass/driver/VPN/firewall). Re-run `flask sync-rgritten`.
2) Add a backfill/reset-cursor option so `--min-ritdatum` can ingest older ranges even if newer data already exists.
3) Consider idempotent upsert if source can change on same `(ritdatum, ritnummer)`; currently append-only.
4) Report builder enhancements: support more datasets, proper grouping/aggregations (currently ordering only), and filter validation.

## Commands / Artifacts
- Sync:
  - `flask sync-rgritten --profile Historie [--chunk-size 1000] [--min-ritdatum YYYY-MM-DD]`
  - `flask diagnose-rgritten --profile Historie [--cursor 0] [--min-ritdatum YYYY-MM-DD]`
  - `flask debug-rgritten-cols --profile Historie`
- DB: `instance/app.db` (rebuilt without UNIQUE(owner_id, ritnummer)); indexes on owner_id, ritnummer.
- Schema: see `models.py` (`RGRit` includes all columns from `rpt.RGRitten`; no uniqueness except PK `id`); `ReportTemplate` stores template JSON fields.
- Migrations: use Flask-Migrate; table rebuild was done directly in SQLite, not via migration file.
- Logs: last failure was `pyodbc OperationalError HYT00 Login timeout expired` when connecting to SQL Server; previous UNIQUE failures resolved by dropping/recreating the table.

## Expected Behavior / Contracts
- Remote view: `rpt.RGRitten` on the `Historie` connection profile.
- Local ingest: `TRY_CONVERT` on all numeric/datetime/time/decimal fields; non-convertible → NULL. Decimals → float before insert.
- Cursor rule: fetch rows where `(ritdatum > last_date) OR (ritdatum == last_date AND ritnummer > last_ritnummer)`, also respecting `--min-ritdatum` if provided.

## Environment
- Python 3.13 (venv: `.venv`).
- Packages (installed): Flask 3.1.0, Flask-Login 0.6.3, Flask-WTF 1.2.2, email-validator 2.2.0, Flask-Migrate 4.1.0, Flask-SQLAlchemy 3.1.1, SQLAlchemy 2.0.40, python-dotenv 1.1.0, passlib 1.7.4, pyodbc 5.2.0, pytest 8.4.2. (gunicorn/waitress not installed in this env.)
- Avoid creating duplicate envs; reuse `.venv`.

## Testing
- `pytest -q` now passes (added repo root to `sys.path` in `tests/conftest.py`).

## File Map to Check
- `app.py` (CLI commands), `rgritten_sync.py` (sync/diagnostics), `models.py` (`RGRit`), `instance/app.db` (rebuilt), `requirements.txt`, `README.md`, `HANDOFF.md`.
