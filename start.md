# Start SmartWheels

This project can run with the built-in Flask dev server, plus production-grade WSGI servers for macOS/Linux (`gunicorn`) and Windows (`waitress`). The commands below assume you already created and activated a virtual environment (`python -m venv .venv`).

## 1. Install dependencies

```bash
pip install -r requirements.txt
```

## 2. Environment configuration

Create a `.env` in the project root (if you do not already have one) with at least:

```
SECRET_KEY=change-me
SQLALCHEMY_DATABASE_URI=sqlite:///instance/app.db
FLASK_APP=app:create_app
```

You can add other settings such as `FLASK_DEBUG=1` during development.

## 3. Database migrations

If you have not initialised Alembic yet:

```bash
flask db init
```

Then generate and apply migrations as normal:

```bash
flask db migrate -m "initial setup"
flask db upgrade
```

## 4. Start options

### Flask development server

Great for local iteration with auto-reload:

```bash
flask --app app:create_app run --debug
```

### Gunicorn (macOS / Linux)

Gunicorn is installed via `requirements.txt`. Use the WSGI entry point provided in `wsgi.py`:

```bash
gunicorn --bind 0.0.0.0:8000 --workers 3 wsgi:app
```

Adjust the `--workers` count to match your CPU cores. Omit `--bind` to use the default `127.0.0.1:8000`.

### Waitress (Windows friendly)

Waitress ships with a CLI entry-point in the same virtual environment. Use it like this:

```powershell
# PowerShell example
env:FLASK_ENV="production"
waitress-serve --listen=0.0.0.0:8000 wsgi:app
```

Or from CMD:

```cmd
set FLASK_ENV=production
waitress-serve --listen=0.0.0.0:8000 wsgi:app
```

Waitress uses a single process with multiple threads by default; tweak settings via flags such as `--threads 6` if needed.

## 5. Verification

Visit `http://localhost:8000/` (or your chosen port) to confirm the app starts. Use the `/init` route while in development to seed the default admin user and roles.

---

Need to stop a server? Use `Ctrl+C` or close the PowerShell/CMD window.
