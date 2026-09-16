import email
import email.policy
from pathlib import Path

import pytest

import charts
import send_report
from insights import build_insights


# ---------------------------------------------------------------- insights
def test_insights_none_when_no_data(memory_db):
    assert build_insights(memory_db, "2026-09-16") is None


def test_insights_first_day_has_no_comparison(two_day_db):
    ins = build_insights(two_day_db, "2026-09-15")
    assert ins["previous_date"] is None
    assert ins["total_market_cap_change_pct"] is None
    assert ins["new_entrants"] == [] and ins["dropouts"] == []
    assert any("comparison starts tomorrow" in a for a in ins["alerts"])
    assert len(ins["top_gainers"]) == 5 and len(ins["top_losers"]) == 5


def test_insights_day_over_day(two_day_db):
    ins = build_insights(two_day_db, "2026-09-16")
    assert ins["previous_date"] == "2026-09-15"
    assert ins["coins"] == 20
    assert ins["total_market_cap_change_pct"] is not None

    assert [r["coin_id"] for r in ins["new_entrants"]] == ["newcoin"]
    assert [r["coin_id"] for r in ins["dropouts"]] == ["coin20"]

    climbers = {r["coin_id"]: r["rank_change"] for r in ins["rank_climbers"]}
    fallers = {r["coin_id"]: r["rank_change"] for r in ins["rank_fallers"]}
    assert climbers["coin4"] == 1
    assert fallers["coin3"] == -1

    # gainers sorted descending, losers ascending
    g = [r["price_change_pct_24h"] for r in ins["top_gainers"]]
    l = [r["price_change_pct_24h"] for r in ins["top_losers"]]
    assert g == sorted(g, reverse=True) and g[0] == 42.0
    assert l == sorted(l)
    assert any("composition changed" in a for a in ins["alerts"])
    assert [h["fetch_date"] for h in ins["history"]] == ["2026-09-15", "2026-09-16"]


def test_insights_market_cap_alert_threshold(two_day_db):
    ins = build_insights(two_day_db, "2026-09-16", alert_pct=0.0001)
    assert any("Total market cap moved" in a for a in ins["alerts"])
    ins = build_insights(two_day_db, "2026-09-16", alert_pct=99)
    assert not any("Total market cap moved" in a for a in ins["alerts"])


# ---------------------------------------------------------------- charts
def test_charts_render_all_three_with_history(two_day_db, tmp_path):
    ins = build_insights(two_day_db, "2026-09-16")
    out = charts.generate_charts(ins, tmp_path)
    assert set(out) == {"movers", "market_cap", "trend"}
    for p in out.values():
        assert Path(p).stat().st_size > 1000


def test_trend_chart_skipped_with_one_day(two_day_db, tmp_path):
    ins = build_insights(two_day_db, "2026-09-15")
    out = charts.generate_charts(ins, tmp_path)
    assert "trend" not in out
    assert {"movers", "market_cap"} <= set(out)


def test_charts_with_no_insights(tmp_path):
    assert charts.generate_charts(None, tmp_path) == {}


@pytest.mark.parametrize("value, text", [
    (1.52e12, "$1.52T"), (291.8e9, "$291.80B"), (33.3e6, "$33.30M"), (950, "$950"), (2e9, "$2B"),
])
def test_compact_usd(value, text):
    assert charts.compact_usd(value) == text


# ---------------------------------------------------------------- report
def _result(status="SUCCESS", **extra):
    base = {
        "status": status, "fetch_date": "2026-09-16", "timestamp": "2026-09-16T09:00:00+00:00",
        "records_fetched": 20, "records_stored": 20 if status == "SUCCESS" else 0,
        "quality_metrics": {"raw_records": 20, "invalid_price": 0, "invalid_market_cap": 0,
                            "invalid_rank": 0, "invalid_change": 0, "stale_records": 0,
                            "duplicates_removed": 0, "clean_records": 20},
        "warnings": [], "error_message": None,
    }
    base.update(extra)
    return base


def test_subject_lines(two_day_db):
    ins = build_insights(two_day_db, "2026-09-16")
    ok = send_report.build_subject(_result(), ins)
    assert "2026-09-16" in ok and "Top-20 cap" in ok and "d/d" in ok
    skipped = send_report.build_subject(_result("SKIPPED"), ins)
    assert "already ingested" in skipped
    failed = send_report.build_subject(_result("FAILED", error_message="boom"), None)
    assert "FAILED" in failed


def test_html_render_success(two_day_db, tmp_path):
    ins = build_insights(two_day_db, "2026-09-16")
    html = send_report.render_html(_result(), ins, {"movers": "cid:x", "market_cap": "cid:y"})
    assert "SUCCESS" in html
    assert "Top gainers" in html and "NEW" in html  # newcomer symbol upper-cased
    assert "Entered top-20" in html and "Dropped out" in html
    assert 'src="cid:x"' in html
    assert "Rank climbers" in html


def test_html_render_failure_without_insights():
    html = send_report.render_html(_result("FAILED", error_message="API <down>"), None, {})
    assert "FAILED" in html
    assert "API &lt;down&gt;" in html  # autoescaped
    assert "Next steps" in html
    assert "Top gainers" not in html


def test_email_is_multipart_with_inline_images(two_day_db, tmp_path):
    ins = build_insights(two_day_db, "2026-09-16")
    paths = charts.generate_charts(ins, tmp_path)
    msg = send_report.build_email(_result(), ins, paths, "me@example.com", ["you@example.com"])

    assert msg["To"] == "you@example.com"
    parsed = email.message_from_bytes(msg.as_bytes(), policy=email.policy.default)
    types = [part.get_content_type() for part in parsed.walk()]
    assert "text/plain" in types and "text/html" in types
    assert types.count("image/png") == len(paths)

    html_body = next(p for p in parsed.walk() if p.get_content_type() == "text/html").get_content()
    image_cids = [p["Content-ID"].strip("<>") for p in parsed.walk() if p.get_content_type() == "image/png"]
    for cid in image_cids:
        assert f"cid:{cid}" in html_body


def test_send_report_dry_run_writes_preview(two_day_db, tmp_path, monkeypatch):
    monkeypatch.setattr(send_report.config, "REPORT_DIR", tmp_path / "reports")
    ins = build_insights(two_day_db, "2026-09-16")
    outcome = send_report.send_report(_result(), ins, {}, dry_run=True)
    assert outcome["sent"] is False
    assert outcome["skipped_reason"] == "dry-run"
    assert Path(outcome["preview"]).exists()


def test_send_report_without_smtp_config_is_preview_only(tmp_path, monkeypatch):
    monkeypatch.setattr(send_report.config, "REPORT_DIR", tmp_path / "reports")
    monkeypatch.setattr(send_report.config, "SMTP_USER", None)
    outcome = send_report.send_report(_result("FAILED", error_message="x"), None, {})
    assert outcome["skipped_reason"] == "smtp-not-configured"


def test_send_report_uses_smtp_when_configured(tmp_path, monkeypatch):
    monkeypatch.setattr(send_report.config, "REPORT_DIR", tmp_path / "reports")
    monkeypatch.setattr(send_report.config, "SMTP_USER", "me@example.com")
    monkeypatch.setattr(send_report.config, "SMTP_PASSWORD", "secret")
    monkeypatch.setattr(send_report.config, "REPORT_FROM", "me@example.com")
    monkeypatch.setattr(send_report.config, "REPORT_TO", ["you@example.com"])
    sent = []
    monkeypatch.setattr(send_report, "send_email", lambda msg: sent.append(msg["Subject"]) or True)
    outcome = send_report.send_report(_result(), None, {})
    assert outcome["sent"] is True
    assert sent and "2026-09-16" in sent[0]
