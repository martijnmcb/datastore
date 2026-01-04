import os
import sys
from pathlib import Path
import pytest

# Ensure project root is on sys.path for `import app`
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

@pytest.fixture(scope="session", autouse=True)
def _set_env():
    os.environ.setdefault("FLASK_DEBUG", "0")
    os.environ.setdefault("SECRET_KEY", "test-secret")
    os.environ.setdefault("SQLALCHEMY_DATABASE_URI", "sqlite:///:memory:")
    yield


@pytest.fixture()
def app():
    from app import create_app
    from extensions import db

    app = create_app()
    with app.app_context():
        db.create_all()
    yield app


@pytest.fixture()
def client(app):
    return app.test_client()
