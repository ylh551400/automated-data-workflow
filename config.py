"""
Central configuration.

Every setting has a sensible default and can be overridden with an environment
variable, so the same code runs locally, in GitHub Actions, or on any scheduler
without editing source files. Secrets (SMTP credentials) are *only* read from
the environment and never hardcoded.
"""

import logging
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


def _env(name: str, default=None):
    value = os.getenv(name)
    return default if value in (None, "") else value


# ------------------------------------------------------------------
# Data source: CoinGecko public API (no key required)
# ------------------------------------------------------------------
API_URL = _env("COINGECKO_API_URL", "https://api.coingecko.com/api/v3/coins/markets")
TOP_N_COINS = int(_env("TOP_N_COINS", 100))
API_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": TOP_N_COINS,
    "page": 1,
    "sparkline": "false",
    "price_change_percentage": "24h,7d",
}
# Optional demo API key raises the rate limit; leave unset for anonymous access.
COINGECKO_API_KEY = _env("COINGECKO_API_KEY")
USER_AGENT = "automated-data-workflow/2.0 (+https://github.com/ylh551400/automated-data-workflow)"

REQUEST_TIMEOUT_SECONDS = int(_env("REQUEST_TIMEOUT_SECONDS", 30))
MAX_RETRIES = int(_env("MAX_RETRIES", 4))
RETRY_BASE_DELAY_SECONDS = float(_env("RETRY_BASE_DELAY_SECONDS", 5))

# ------------------------------------------------------------------
# Storage
# ------------------------------------------------------------------
DB_PATH = Path(_env("DB_PATH", BASE_DIR / "data" / "market_snapshots.db"))
TABLE_NAME = "market_snapshots"

# ------------------------------------------------------------------
# Data quality thresholds
# ------------------------------------------------------------------
MIN_EXPECTED_RECORDS = int(_env("MIN_EXPECTED_RECORDS", 90))  # alert if fewer clean rows
STALE_HOURS = int(_env("STALE_HOURS", 24))                     # drop rows not updated recently
MARKET_CAP_ALERT_PCT = float(_env("MARKET_CAP_ALERT_PCT", 5.0))  # flag big day-over-day swings

# ------------------------------------------------------------------
# Output locations
# ------------------------------------------------------------------
CHART_DIR = Path(_env("CHART_DIR", BASE_DIR / "charts"))
REPORT_DIR = Path(_env("REPORT_DIR", BASE_DIR / "reports"))
LOG_FILE = Path(_env("LOG_FILE", BASE_DIR / "pipeline.log"))

# ------------------------------------------------------------------
# Email (all from environment; see .env.example)
# ------------------------------------------------------------------
SMTP_HOST = _env("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(_env("SMTP_PORT", 587))
SMTP_USER = _env("SMTP_USER")
SMTP_PASSWORD = _env("SMTP_PASSWORD")
REPORT_FROM = _env("REPORT_FROM", SMTP_USER)
REPORT_TO = [addr.strip() for addr in _env("REPORT_TO", "").split(",") if addr.strip()]
REPORT_TITLE = _env("REPORT_TITLE", "Daily Crypto Market Report")


def email_configured() -> bool:
    """True when enough SMTP settings exist to actually send mail."""
    return bool(SMTP_USER and SMTP_PASSWORD and REPORT_TO)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging once for the whole process (file + console)."""
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    # Windows consoles may default to a legacy code page; keep emoji in subjects from crashing logs.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )
