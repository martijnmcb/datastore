import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")
    SQLALCHEMY_DATABASE_URI = os.getenv("SQLALCHEMY_DATABASE_URI", "sqlite:///app.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Daily data refresh scheduler (disabled by default)
    DATA_REFRESH_ENABLED = os.getenv("DATA_REFRESH_ENABLED", "0") == "1"
    DATA_REFRESH_TIME = os.getenv("DATA_REFRESH_TIME", "02:00")  # HH:MM local time
    DATA_REFRESH_PROFILE = os.getenv("DATA_REFRESH_PROFILE", "Historie")
    DATA_REFRESH_CHUNK_SIZE = int(os.getenv("DATA_REFRESH_CHUNK_SIZE", "1000"))
    DATA_REFRESH_MIN_RITDATUM = os.getenv("DATA_REFRESH_MIN_RITDATUM") or None
