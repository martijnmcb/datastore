## Environment
- Create venv: `python -m venv .venv && source .venv/bin/activate` (Windows: `.venv\\Scripts\\activate`).
- Install deps: `pip install -r requirements.txt`.

## Run app
- Dev server: `flask --app app:create_app run` (or `python app.py`).
- Seed roles/admin (dev only): run server then hit `http://localhost:5000/init` (use `INIT_TOKEN` if set).

## Database
- Migrations: `flask db init` (once) → `flask db migrate -m "msg"` → `flask db upgrade`.
- Local DB path: `instance/app.db`.

## RGRitten sync/diagnostics
- Sync: `flask sync-rgritten --profile Historie [--chunk-size 1000] [--min-ritdatum YYYY-MM-DD]`.
- Diagnose: `flask diagnose-rgritten --profile Historie [--cursor 0] [--min-ritdatum YYYY-MM-DD]`.
- Inspect columns: `flask debug-rgritten-cols --profile Historie`.

## Tests
- Run: `pytest -q` (currently failing due to import path `ModuleNotFoundError: app`).
