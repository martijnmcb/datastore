# Repository Guidelines

## Project Structure & Module Organization
- `app.py`: App factory, dev server, and a guarded `/init` bootstrap route.
- `blueprints/`: Feature modules — `auth`, `main`, `admin` (routes in `routes.py`).
- `models.py`: SQLAlchemy models (`User`, `Role`, connection profiles).
- `extensions.py`: App-wide instances (DB, login, migrations).
- `config.py` + `.env`: Runtime config (e.g., `SECRET_KEY`, `SQLALCHEMY_DATABASE_URI`).
- `templates/`, `static/`: Jinja templates and assets.
- `instance/`: Local artifacts (e.g., `app.db`) for development only.

## Build, Test, and Development Commands
- Python: 3.11+ recommended.
- Create env: `python -m venv .venv && source .venv/bin/activate` (Windows: `.venv\\Scripts\\activate`).
- Install deps: `pip install -r requirements.txt`.
- Run locally: `flask --app app:create_app run` (or `python app.py`).
- Debug toggle: `export FLASK_DEBUG=1` to enable debug.
- DB migrations: `flask db init` (once) → `flask db migrate -m "msg"` → `flask db upgrade`.
- Seed roles/admin (dev only): visit `http://localhost:5000/init` or add `?token=...` if configured.

## Coding Style & Naming Conventions
- Python style: PEP 8, 4-space indent, 100-char line target.
- Names: `snake_case` for functions/vars, `PascalCase` for classes; blueprint names match folder.
- Blueprints: keep routes in `routes.py`; register via `app.py` with clear prefixes (`/auth`, `/beheer`).
- Templates: co-locate partials and use consistent prefixes (e.g., `admin_*.html`).

## Testing Guidelines
- Framework: pytest (recommended).
- Location: `tests/test_*.py`; create the app with `from app import create_app`.
- Example: `client = create_app().test_client()` to test routes.
- Run: `pytest -q`; target auth, role checks, and admin flows first.

## Commit & Pull Request Guidelines
- Commits: imperative, scoped, concise. Example: `feat(admin): add user toggle with role check`.
- Include: what/why, affected blueprints/models, and migration notes if applicable.
- PRs: link issues, list steps to test, add screenshots for UI changes (`templates/`), and note config/env impacts.

## Security & Configuration Tips
- Configure via `.env`: `SECRET_KEY`, `SQLALCHEMY_DATABASE_URI` (SQLite for dev; MSSQL via `pyodbc` in prod).
- Never commit secrets, `.env`, or local DBs in `instance/`.
- The `/init` route is for development/bootstrap; keep it disabled in production or require `INIT_TOKEN`.
