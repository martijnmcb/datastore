## Project purpose
Lightweight Flask app providing auth/admin plus a report-builder that syncs SQL Server view `rpt.RGRitten` into local SQLite table `rgritten` for reporting.

## Architecture & structure
- `app.py`: app factory, CLI commands, guarded `/init` bootstrap route.
- Blueprints in `blueprints/`: `auth`, `admin`, `main`, `reports` (routes live in each `routes.py`).
- Data layer: `models.py` (User, Role, ConnectionProfile, RGRit, etc.), `extensions.py` (db/login/migrate instances).
- Config via `.env` read by `config.py`; local artifacts in `instance/` (e.g., `app.db`).
- Templates in `templates/`, assets in `static/`.
- Sync helper `rgritten_sync.py` for pulling `rpt.RGRitten` into SQLite; CLI wired via `app.py`.
- WSGI entrypoint `wsgi.py`; migrations folder present.

## Tech stack
- Python 3.11+ (README notes 3.13 venv), Flask 3.1, Flask-SQLAlchemy, Flask-Migrate, Flask-Login, Flask-WTF, email-validator, passlib, SQLAlchemy 2.0, pyodbc. Tests: pytest. Deploy servers listed (gunicorn/waitress in requirements).

## Key behaviors
- `/init` seeds roles/admin; should be disabled or token-protected in production.
- RGRitten sync is append-only keyed by `(ritdatum, ritnummer)`; forward-only cursor. Remote SQL Server profile `Historie` needed.
- Report builder lives under `/reports`; templates persisted in `report_templates`.
- Current known issues: SQL Server login timeout for Historie profile; pytest failing due to import path (`ModuleNotFoundError: app`).
