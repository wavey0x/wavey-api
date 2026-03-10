import os
from dotenv import load_dotenv
load_dotenv()


def _get_required_env(name):
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"{name} must be set")
    return value.strip()


def _get_int_env(name, default):
    value = os.getenv(name)
    if value is None or not value.strip():
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc


class Config:
    def __init__(self):
        self.SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URI')
        self.SQLALCHEMY_TRACK_MODIFICATIONS = False
        self.WEB3_INFURA_PROJECT_ID = os.getenv('WEB3_INFURA_PROJECT_ID')
        self.INFURA_API_KEY = os.getenv('WEB3_INFURA_PROJECT_ID')
        self.ADMIN_API_KEY = os.environ.get('ADMIN_API_KEY', 'changeme')
        self.FACTORY_DASHBOARD_DB_PATH = _get_required_env('FACTORY_DASHBOARD_DB_PATH')
        self.FACTORY_DASHBOARD_BUSY_TIMEOUT_MS = _get_int_env('FACTORY_DASHBOARD_BUSY_TIMEOUT_MS', 5000)
        self.FACTORY_DASHBOARD_CACHE_MAX_AGE_SECONDS = _get_int_env('FACTORY_DASHBOARD_CACHE_MAX_AGE_SECONDS', 30)
