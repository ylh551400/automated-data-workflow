from datetime import timedelta

import pytest
import requests

import data_pipeline as dp
from conftest import make_market, make_record


# ---------------------------------------------------------------- schema
def test_validate_schema_passes_on_complete_records():
    assert dp.validate_schema(make_market(3)) == []


def test_validate_schema_reports_missing_fields():
    records = make_market(3)
    del records[1]["market_cap"]
    del records[2]["last_updated"]
    assert dp.validate_schema(records) == ["last_updated", "market_cap"]


def test_validate_schema_empty_payload_fails():
    assert dp.validate_schema([]) == dp.REQUIRED_FIELDS


# ---------------------------------------------------------------- cleaning
def test_clean_keeps_valid_records(now):
    df, metrics = dp.clean_and_validate(make_market(10), "2026-09-16", now=now)
    assert len(df) == 10
    assert metrics["clean_records"] == 10
    assert metrics["raw_records"] == 10
    assert set(df["fetch_date"]) == {"2026-09-16"}
    assert list(df["market_cap_rank"]) == list(range(1, 11))
    assert df["symbol"].iloc[0] == "C1"  # upper-cased


@pytest.mark.parametrize(
    "override, metric",
    [
        ({"current_price": 0}, "invalid_price"),
        ({"current_price": None}, "invalid_price"),
        ({"current_price": "abc"}, "invalid_price"),
        ({"market_cap": None}, "invalid_market_cap"),
        ({"market_cap": -5}, "invalid_market_cap"),
        ({"market_cap_rank": None}, "invalid_rank"),
        ({"price_change_percentage_24h": None}, "invalid_change"),
        ({"price_change_percentage_24h": -100}, "invalid_change"),
        ({"last_updated": "not-a-date"}, "stale_records"),
    ],
)
def test_clean_filters_invalid_record(now, override, metric):
    bad = {"id": "bad", "symbol": "bad", "market_cap_rank": 99, **override}
    records = make_market(5) + [make_record(**bad)]
    df, metrics = dp.clean_and_validate(records, "2026-09-16", now=now)
    assert metrics[metric] == 1
    assert metrics["clean_records"] == 5
    assert "bad" not in set(df["coin_id"])


def test_clean_filters_stale_records(now):
    stale = make_record(id="old", symbol="old", market_cap_rank=50,
                        last_updated=(now - timedelta(hours=30)).isoformat())
    df, metrics = dp.clean_and_validate(make_market(5) + [stale], "2026-09-16", now=now, stale_hours=24)
    assert metrics["stale_records"] == 1
    assert "old" not in set(df["coin_id"])


def test_clean_removes_duplicate_ids(now):
    records = make_market(5)
    records.append(dict(records[0]))
    df, metrics = dp.clean_and_validate(records, "2026-09-16", now=now)
    assert metrics["duplicates_removed"] == 1
    assert df["coin_id"].is_unique


def test_clean_tolerates_missing_optional_fields(now):
    records = make_market(3)
    for r in records:
        del r["high_24h"]
        del r["price_change_percentage_7d_in_currency"]
    df, metrics = dp.clean_and_validate(records, "2026-09-16", now=now)
    assert metrics["clean_records"] == 3
    assert df["high_24h"].isna().all()


# ---------------------------------------------------------------- storage
def test_store_is_idempotent_per_day(memory_db, now):
    df, _ = dp.clean_and_validate(make_market(5), "2026-09-16", now=now)
    assert dp.store_snapshot(df, memory_db) == 5
    assert dp.store_snapshot(df, memory_db) == 0  # second run skipped
    assert dp.snapshot_exists(memory_db, "2026-09-16") == 5


def test_store_force_replaces_existing_day(memory_db, now):
    df, _ = dp.clean_and_validate(make_market(5), "2026-09-16", now=now)
    dp.store_snapshot(df, memory_db)
    df2, _ = dp.clean_and_validate(make_market(7), "2026-09-16", now=now)
    assert dp.store_snapshot(df2, memory_db, force=True) == 7
    assert dp.snapshot_exists(memory_db, "2026-09-16") == 7


def test_store_keeps_separate_days(memory_db, now):
    for date in ("2026-09-15", "2026-09-16"):
        df, _ = dp.clean_and_validate(make_market(5), date, now=now)
        dp.store_snapshot(df, memory_db)
    total = memory_db.execute(f"SELECT COUNT(*) FROM {dp.config.TABLE_NAME}").fetchone()[0]
    assert total == 10


# ---------------------------------------------------------------- fetch/retry
class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else []
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code}")

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_fetch_retries_on_server_error_then_succeeds():
    session = FakeSession([FakeResponse(503), FakeResponse(200, make_market(2))])
    sleeps = []
    data = dp.fetch_market_data(session=session, max_retries=3, base_delay=1, sleep=sleeps.append)
    assert len(data) == 2
    assert session.calls == 2
    assert sleeps == [1]


def test_fetch_honours_retry_after_on_429():
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, [])])
    sleeps = []
    dp.fetch_market_data(session=session, max_retries=2, base_delay=1, sleep=sleeps.append)
    assert sleeps == [7]


def test_fetch_retries_on_timeout_with_backoff():
    session = FakeSession([requests.exceptions.Timeout(), requests.exceptions.ConnectionError(),
                           FakeResponse(200, [])])
    sleeps = []
    dp.fetch_market_data(session=session, max_retries=3, base_delay=2, sleep=sleeps.append)
    assert sleeps == [2, 4]


def test_fetch_gives_up_after_max_retries():
    session = FakeSession([FakeResponse(500)] * 3)
    with pytest.raises(RuntimeError, match="after 3 attempts"):
        dp.fetch_market_data(session=session, max_retries=3, base_delay=0, sleep=lambda _: None)
    assert session.calls == 3


def test_fetch_does_not_retry_client_errors():
    session = FakeSession([FakeResponse(404), FakeResponse(200, [])])
    with pytest.raises(requests.exceptions.HTTPError):
        dp.fetch_market_data(session=session, max_retries=3, base_delay=0, sleep=lambda _: None)
    assert session.calls == 1


def test_fetch_rejects_non_list_payload():
    session = FakeSession([FakeResponse(200, {"error": "nope"})])
    with pytest.raises(ValueError):
        dp.fetch_market_data(session=session, max_retries=3, base_delay=0, sleep=lambda _: None)


# ---------------------------------------------------------------- end to end (no network)
def test_run_pipeline_success_and_skip(monkeypatch, tmp_path):
    monkeypatch.setattr(dp, "fetch_market_data", lambda *a, **k: make_market(95))
    db = tmp_path / "db.sqlite"

    first = dp.run_pipeline(db_path=db)
    assert first["status"] == "SUCCESS"
    assert first["records_stored"] == 95
    assert first["warnings"] == []

    second = dp.run_pipeline(db_path=db)
    assert second["status"] == "SKIPPED"
    assert second["records_stored"] == 0

    forced = dp.run_pipeline(force=True, db_path=db)
    assert forced["status"] == "SUCCESS"


def test_run_pipeline_warns_on_low_record_count(monkeypatch, tmp_path):
    monkeypatch.setattr(dp, "fetch_market_data", lambda *a, **k: make_market(5))
    result = dp.run_pipeline(db_path=tmp_path / "db.sqlite")
    assert result["status"] == "SUCCESS"
    assert any("expected at least" in w for w in result["warnings"])


def test_run_pipeline_fails_on_schema_change(monkeypatch, tmp_path):
    broken = make_market(5)
    for r in broken:
        del r["market_cap"]
    monkeypatch.setattr(dp, "fetch_market_data", lambda *a, **k: broken)
    result = dp.run_pipeline(db_path=tmp_path / "db.sqlite")
    assert result["status"] == "FAILED"
    assert "market_cap" in result["error_message"]


def test_run_pipeline_reports_fetch_failure(monkeypatch, tmp_path):
    def boom(*a, **k):
        raise RuntimeError("API down")

    monkeypatch.setattr(dp, "fetch_market_data", boom)
    result = dp.run_pipeline(db_path=tmp_path / "db.sqlite")
    assert result["status"] == "FAILED"
    assert result["error_message"] == "API down"
