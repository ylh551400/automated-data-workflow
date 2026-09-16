"""
Data pipeline: Extract -> Validate -> Transform -> Load

- Fetches a top-N market snapshot from the CoinGecko public API (with retry)
- Validates the response schema so upstream API changes fail loudly
- Applies data-quality rules and records what was filtered and why
- Stores one snapshot per UTC day in SQLite (idempotent, re-runnable with --force)
"""

import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

import config

logger = logging.getLogger(__name__)

# Fields the API must return. Missing any of these means the schema changed.
REQUIRED_FIELDS = [
    "id",
    "symbol",
    "name",
    "market_cap_rank",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_percentage_24h",
    "last_updated",
]
# Nice-to-have fields; filled with NULL when absent.
OPTIONAL_FIELDS = [
    "high_24h",
    "low_24h",
    "circulating_supply",
    "price_change_percentage_7d_in_currency",
]
# API field name -> database column name
COLUMN_MAP = {
    "id": "coin_id",
    "symbol": "symbol",
    "name": "name",
    "market_cap_rank": "market_cap_rank",
    "current_price": "current_price",
    "market_cap": "market_cap",
    "total_volume": "total_volume",
    "price_change_percentage_24h": "price_change_pct_24h",
    "price_change_percentage_7d_in_currency": "price_change_pct_7d",
    "high_24h": "high_24h",
    "low_24h": "low_24h",
    "circulating_supply": "circulating_supply",
    "last_updated": "last_updated",
}
NUMERIC_COLUMNS = [
    "market_cap_rank",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_pct_24h",
    "price_change_pct_7d",
    "high_24h",
    "low_24h",
    "circulating_supply",
]

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {config.TABLE_NAME} (
    fetch_date            TEXT    NOT NULL,
    coin_id               TEXT    NOT NULL,
    symbol                TEXT,
    name                  TEXT,
    market_cap_rank       INTEGER,
    current_price         REAL,
    market_cap            REAL,
    total_volume          REAL,
    price_change_pct_24h  REAL,
    price_change_pct_7d   REAL,
    high_24h              REAL,
    low_24h               REAL,
    circulating_supply    REAL,
    last_updated          TEXT,
    ingested_at           TEXT    NOT NULL,
    PRIMARY KEY (fetch_date, coin_id)
)
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def today_utc() -> str:
    return utc_now().strftime("%Y-%m-%d")


# ============================================================
# EXTRACT
# ============================================================
def fetch_market_data(
    url: str = config.API_URL,
    params: dict | None = None,
    max_retries: int = config.MAX_RETRIES,
    base_delay: float = config.RETRY_BASE_DELAY_SECONDS,
    session: requests.Session | None = None,
    sleep=time.sleep,
) -> list[dict]:
    """
    Fetch the market snapshot with exponential backoff.

    Retries on timeouts, connection errors, HTTP 429 (honouring Retry-After)
    and 5xx responses. Other 4xx responses and malformed JSON are not
    retried because retrying cannot fix them.
    """
    params = config.API_PARAMS if params is None else params
    session = session or requests.Session()
    headers = {"Accept": "application/json", "User-Agent": config.USER_AGENT}
    if config.COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = config.COINGECKO_API_KEY

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        delay = base_delay * (2 ** (attempt - 1))
        try:
            logger.info("API fetch attempt %s/%s", attempt, max_retries)
            response = session.get(
                url, params=params, headers=headers, timeout=config.REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    delay = max(delay, float(retry_after))
                last_error = RuntimeError("HTTP 429 rate limited")
                logger.warning("Attempt %s: rate limited (429)", attempt)
            elif response.status_code >= 500:
                last_error = RuntimeError(f"HTTP {response.status_code} server error")
                logger.warning("Attempt %s: server error %s", attempt, response.status_code)
            else:
                response.raise_for_status()  # remaining 4xx -> HTTPError (not retried)
                data = response.json()
                if not isinstance(data, list):
                    raise ValueError(f"Unexpected payload type: {type(data).__name__}")
                logger.info("Fetched %s records from API", len(data))
                return data

        except requests.exceptions.HTTPError as exc:
            logger.error("Non-retryable HTTP error: %s", exc)
            raise
        except ValueError as exc:
            logger.error("Invalid JSON response: %s", exc)
            raise
        except requests.exceptions.RequestException as exc:
            last_error = exc
            logger.warning("Attempt %s: request failed - %s", attempt, exc)

        if attempt < max_retries:
            logger.info("Retrying in %.0f seconds...", delay)
            sleep(delay)

    raise RuntimeError(f"API fetch failed after {max_retries} attempts: {last_error}")


# ============================================================
# SCHEMA VALIDATION
# ============================================================
def validate_schema(data: list[dict], required_fields: list[str] = REQUIRED_FIELDS) -> list[str]:
    """
    Return the list of required fields missing from at least one record.
    An empty list means the schema is intact. An empty payload counts as
    every field missing.
    """
    if not data:
        logger.error("Schema validation failed: empty response")
        return list(required_fields)

    missing: set[str] = set()
    for record in data:
        if not isinstance(record, dict):
            return list(required_fields)
        missing.update(f for f in required_fields if f not in record)

    if missing:
        logger.error("Schema validation failed: missing fields %s", sorted(missing))
    else:
        logger.info("Schema validation passed (%s records)", len(data))
    return sorted(missing)


# ============================================================
# TRANSFORM + DATA QUALITY
# ============================================================
def clean_and_validate(
    data: list[dict],
    fetch_date: str,
    now: datetime | None = None,
    stale_hours: int = config.STALE_HOURS,
) -> tuple[pd.DataFrame, dict]:
    """
    Normalise the raw API payload and apply data-quality rules.

    Rules (a record failing any rule is dropped and counted):
      1. current_price must be a positive number
      2. market_cap must be a positive number
      3. market_cap_rank must be present
      4. price_change_pct_24h must be present and > -100
      5. last_updated must be within `stale_hours` of now
      6. coin_id must be unique within the batch

    Returns (clean_dataframe, metrics).
    """
    now = now or utc_now()
    df = pd.DataFrame(data)

    for field in OPTIONAL_FIELDS:
        if field not in df.columns:
            df[field] = None
    df = df[list(COLUMN_MAP)].rename(columns=COLUMN_MAP)

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["symbol"] = df["symbol"].astype(str).str.upper()

    metrics = {
        "raw_records": len(df),
        "invalid_price": 0,
        "invalid_market_cap": 0,
        "invalid_rank": 0,
        "invalid_change": 0,
        "stale_records": 0,
        "duplicates_removed": 0,
        "clean_records": 0,
    }

    mask = df["current_price"].isna() | (df["current_price"] <= 0)
    metrics["invalid_price"] = int(mask.sum())
    df = df[~mask]

    mask = df["market_cap"].isna() | (df["market_cap"] <= 0)
    metrics["invalid_market_cap"] = int(mask.sum())
    df = df[~mask]

    mask = df["market_cap_rank"].isna()
    metrics["invalid_rank"] = int(mask.sum())
    df = df[~mask]

    mask = df["price_change_pct_24h"].isna() | (df["price_change_pct_24h"] <= -100)
    metrics["invalid_change"] = int(mask.sum())
    df = df[~mask]

    updated = pd.to_datetime(df["last_updated"], utc=True, errors="coerce")
    cutoff = now - timedelta(hours=stale_hours)
    mask = updated.isna() | (updated < cutoff)
    metrics["stale_records"] = int(mask.sum())
    df = df[~mask]

    dup_mask = df.duplicated(subset=["coin_id"], keep="first")
    metrics["duplicates_removed"] = int(dup_mask.sum())
    df = df[~dup_mask]

    df = df.copy()
    df["market_cap_rank"] = df["market_cap_rank"].astype(int)
    df.insert(0, "fetch_date", fetch_date)
    df["ingested_at"] = now.isoformat(timespec="seconds")
    df = df.sort_values("market_cap_rank").reset_index(drop=True)

    metrics["clean_records"] = len(df)
    filtered = metrics["raw_records"] - metrics["clean_records"]
    logger.info("Data cleaning complete: %s/%s records passed", len(df), metrics["raw_records"])
    if filtered:
        logger.warning(
            "Filtered %s records: price=%s, market_cap=%s, rank=%s, change=%s, stale=%s, duplicates=%s",
            filtered, metrics["invalid_price"], metrics["invalid_market_cap"], metrics["invalid_rank"],
            metrics["invalid_change"], metrics["stale_records"], metrics["duplicates_removed"],
        )
    return df, metrics


# ============================================================
# LOAD
# ============================================================
def connect(db_path: Path | str = config.DB_PATH) -> sqlite3.Connection:
    """Open (and initialise) the SQLite database."""
    db_path = Path(db_path)
    if str(db_path) != ":memory:":
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    return conn


def snapshot_exists(conn: sqlite3.Connection, fetch_date: str) -> int:
    """Number of rows already stored for `fetch_date` (idempotency check)."""
    row = conn.execute(
        f"SELECT COUNT(*) FROM {config.TABLE_NAME} WHERE fetch_date = ?", (fetch_date,)
    ).fetchone()
    return int(row[0])


def store_snapshot(df: pd.DataFrame, conn: sqlite3.Connection, force: bool = False) -> int:
    """
    Append the snapshot. Returns rows written, or 0 when the snapshot for
    that date already exists and `force` is False. With `force`, the
    existing rows are replaced atomically.
    """
    if df.empty:
        logger.warning("Nothing to store: cleaned dataframe is empty")
        return 0

    fetch_date = df["fetch_date"].iloc[0]
    existing = snapshot_exists(conn, fetch_date)
    if existing and not force:
        logger.warning(
            "Idempotency check: %s rows already stored for %s - skipping (use --force to replace)",
            existing, fetch_date,
        )
        return 0

    with conn:  # transaction: delete + insert succeed or fail together
        if existing:
            conn.execute(f"DELETE FROM {config.TABLE_NAME} WHERE fetch_date = ?", (fetch_date,))
            logger.info("Replaced %s existing rows for %s", existing, fetch_date)
        df.to_sql(config.TABLE_NAME, conn, if_exists="append", index=False)

    logger.info("Stored %s rows for %s", len(df), fetch_date)
    return len(df)


# ============================================================
# ORCHESTRATION
# ============================================================
def run_pipeline(force: bool = False, db_path: Path | str = config.DB_PATH) -> dict:
    """
    Execute the full ETL and return a result dict consumed by the report.
    status: SUCCESS | SKIPPED | FAILED
    """
    fetch_date = today_utc()
    result = {
        "status": "FAILED",
        "fetch_date": fetch_date,
        "timestamp": utc_now().isoformat(timespec="seconds"),
        "records_fetched": 0,
        "records_stored": 0,
        "quality_metrics": {},
        "warnings": [],
        "error_message": None,
    }
    logger.info("=" * 50)
    logger.info("STARTING DATA PIPELINE for %s", fetch_date)
    logger.info("=" * 50)

    try:
        raw = fetch_market_data()
        result["records_fetched"] = len(raw)

        missing = validate_schema(raw)
        if missing:
            raise ValueError(f"Schema validation failed - missing fields: {missing}")

        clean_df, metrics = clean_and_validate(raw, fetch_date)
        result["quality_metrics"] = metrics

        if len(clean_df) < config.MIN_EXPECTED_RECORDS:
            msg = (f"Only {len(clean_df)} clean records, expected at least "
                   f"{config.MIN_EXPECTED_RECORDS}")
            logger.warning("ALERT: %s", msg)
            result["warnings"].append(msg)

        conn = connect(db_path)
        try:
            stored = store_snapshot(clean_df, conn, force=force)
        finally:
            conn.close()
        result["records_stored"] = stored
        result["status"] = "SUCCESS" if stored else "SKIPPED"

    except Exception as exc:  # noqa: BLE001 - every failure must reach the report
        result["status"] = "FAILED"
        result["error_message"] = str(exc)
        logger.exception("Pipeline failed: %s", exc)

    logger.info("PIPELINE FINISHED - status=%s stored=%s", result["status"], result["records_stored"])
    return result


if __name__ == "__main__":
    import sys

    config.setup_logging()
    outcome = run_pipeline(force="--force" in sys.argv)
    print(f"\nPipeline result: {outcome['status']} "
          f"({outcome['records_stored']} stored / {outcome['records_fetched']} fetched)")
    sys.exit(0 if outcome["status"] != "FAILED" else 1)
