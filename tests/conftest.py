"""Shared fixtures: synthetic CoinGecko-style records and an in-memory database."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_pipeline import clean_and_validate, connect, store_snapshot  # noqa: E402

NOW = datetime(2026, 9, 16, 9, 0, tzinfo=timezone.utc)


def make_record(**overrides) -> dict:
    """One valid API record; override any field to make it invalid."""
    base = {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "market_cap_rank": 1,
        "current_price": 75000.0,
        "market_cap": 1.5e12,
        "total_volume": 3.3e10,
        "price_change_percentage_24h": -0.8,
        "price_change_percentage_7d_in_currency": -4.0,
        "high_24h": 77000.0,
        "low_24h": 75000.0,
        "circulating_supply": 2.0e7,
        "last_updated": (NOW - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
    }
    base.update(overrides)
    return base


def make_market(n: int = 20, day_offset: int = 0) -> list[dict]:
    """A synthetic top-N market. day_offset perturbs prices so days differ."""
    records = []
    for rank in range(1, n + 1):
        drift = 1 + 0.01 * day_offset * ((rank % 3) - 1)
        records.append(
            make_record(
                id=f"coin{rank}",
                symbol=f"c{rank}",
                name=f"Coin {rank}",
                market_cap_rank=rank,
                current_price=1000.0 / rank * drift,
                market_cap=1e12 / rank * drift,
                total_volume=1e10 / rank,
                price_change_percentage_24h=((rank * 7) % 41) - 20.0,  # deterministic, -20..+20
            )
        )
    return records


@pytest.fixture
def now():
    return NOW


@pytest.fixture
def memory_db():
    conn = connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def two_day_db(memory_db):
    """Database with a snapshot for yesterday and today (with a rank swap and one dropout)."""
    yesterday = make_market(20, day_offset=0)
    today = make_market(20, day_offset=1)
    # coin20 leaves the top set, a newcomer arrives; coin3 and coin4 swap ranks
    today[-1] = make_record(id="newcoin", symbol="new", name="New Coin", market_cap_rank=20,
                            current_price=5.0, market_cap=1e12 / 20, total_volume=1e8,
                            price_change_percentage_24h=42.0)
    today[2]["market_cap_rank"], today[3]["market_cap_rank"] = 4, 3

    for date, records in (("2026-09-15", yesterday), ("2026-09-16", today)):
        df, _ = clean_and_validate(records, date, now=NOW)
        store_snapshot(df, memory_db)
    return memory_db
