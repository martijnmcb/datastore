## Before handing off changes
- Run relevant tests: `pytest -q` (note current import path failure; mention if unresolved).
- If touching DB models/migrations, run `flask db migrate -m "msg"` and `flask db upgrade` or note pending migration work.
- For RGRitten sync changes, consider a dry run of `flask sync-rgritten --profile Historie --chunk-size 10 --min-ritdatum ...` or `diagnose-rgritten` and record outcomes if possible.
- Verify app starts: `flask --app app:create_app run` (or `python app.py`) for smoke check.
- Document config/env impacts (e.g., `.env` variables like `SECRET_KEY`, `SQLALCHEMY_DATABASE_URI`, `INIT_TOKEN`).
- Avoid committing secrets or `instance/` artifacts.
