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


def _get_bool_env(name, default):
    value = os.getenv(name)
    if value is None or not value.strip():
        return default

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise RuntimeError(f"{name} must be a boolean")


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
        self.FACTORY_DASHBOARD_DEPLOY_CHAIN_ID = _get_int_env('FACTORY_DASHBOARD_DEPLOY_CHAIN_ID', 1)
        self.FACTORY_DASHBOARD_DEPLOY_AUCTION_FACTORY_ADDRESS = os.getenv(
            'FACTORY_DASHBOARD_DEPLOY_AUCTION_FACTORY_ADDRESS',
            '0xbA7FCb508c7195eE5AE823F37eE2c11D7ED52F8e',
        )
        self.FACTORY_DASHBOARD_DEPLOY_GOVERNANCE_ADDRESS = os.getenv(
            'FACTORY_DASHBOARD_DEPLOY_GOVERNANCE_ADDRESS',
            '0xb634316E06cC0B358437CbadD4dC94F1D3a92B3b',
        )
        self.FACTORY_DASHBOARD_DEPLOY_START_PRICE_BUFFER_BPS = _get_int_env(
            'FACTORY_DASHBOARD_DEPLOY_START_PRICE_BUFFER_BPS',
            1000,
        )
        self.FACTORY_DASHBOARD_DEPLOY_REQUIRE_CURVE_QUOTE = _get_bool_env(
            'FACTORY_DASHBOARD_DEPLOY_REQUIRE_CURVE_QUOTE',
            True,
        )
        self.FACTORY_DASHBOARD_DEPLOY_PRICE_BASE_URL = os.getenv(
            'FACTORY_DASHBOARD_DEPLOY_PRICE_BASE_URL',
            'https://prices.wavey.info',
        )
        self.FACTORY_DASHBOARD_DEPLOY_PRICE_API_KEY = os.getenv(
            'FACTORY_DASHBOARD_DEPLOY_PRICE_API_KEY'
        ) or os.getenv('TOKEN_PRICE_AGG_KEY')
        self.FACTORY_DASHBOARD_DEPLOY_PRICE_TIMEOUT_SECONDS = _get_int_env(
            'FACTORY_DASHBOARD_DEPLOY_PRICE_TIMEOUT_SECONDS',
            10,
        )
