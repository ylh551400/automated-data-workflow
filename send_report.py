"""
Report delivery: renders an HTML email (charts inline) and sends it over SMTP.

- Subject line encodes status + headline numbers so the inbox alone is a dashboard
- HTML body with a plain-text alternative for clients that block HTML
- Charts embedded as related MIME parts (no external image hosting needed)
- Always writes a browser-viewable preview to reports/latest_report.html
- Credentials come from environment variables only (see config.py)
"""

import logging
import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

import config
from charts import compact_usd

logger = logging.getLogger(__name__)

TEMPLATE_DIR = config.BASE_DIR / "templates"

STATUS_STYLE = {
    "SUCCESS": {"bg": "#0ca30c", "fg": "#ffffff", "icon": "✅", "label": "Pipeline succeeded"},
    "SKIPPED": {"bg": "#fab219", "fg": "#0b0b0b", "icon": "⚠️", "label": "Snapshot already stored today"},
    "FAILED": {"bg": "#d03b3b", "fg": "#ffffff", "icon": "\U0001f6a8", "label": "Pipeline failed - action required"},
}


# ------------------------------------------------------------------
# Formatting helpers (exposed to the template)
# ------------------------------------------------------------------
def fmt_pct(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.{digits}f}%"


def fmt_price(value) -> str:
    if value is None:
        return "n/a"
    if value >= 1000:
        return f"${value:,.0f}"
    if value >= 1:
        return f"${value:,.2f}"
    return f"${value:.4g}"


def fmt_int(value) -> str:
    return "n/a" if value is None else f"{int(value):,}"


def delta_color(value) -> str:
    """Ink colour for a signed delta (never the series colour)."""
    if value is None:
        return "#52514e"
    return "#006300" if value >= 0 else "#b32d2d"


# ------------------------------------------------------------------
# Content
# ------------------------------------------------------------------
def build_subject(pipeline_result: dict, insights: dict | None) -> str:
    date = pipeline_result.get("fetch_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    status = pipeline_result.get("status", "UNKNOWN")
    icon = STATUS_STYLE.get(status, {}).get("icon", "\U0001f4ca")

    if status == "FAILED":
        return f"{icon} [{config.REPORT_TITLE}] Pipeline FAILED - {date}"

    parts = [f"{icon} {config.REPORT_TITLE} {date}"]
    if insights:
        if insights.get("btc_price") is not None:
            parts.append(
                f"BTC {compact_usd(insights['btc_price'])} ({fmt_pct(insights['btc_change_pct_24h'], 1)})"
            )
        cap = compact_usd(insights["total_market_cap"])
        change = insights.get("total_market_cap_change_pct")
        suffix = f" ({fmt_pct(change, 1)} d/d)" if change is not None else ""
        parts.append(f"Top-{insights['coins']} cap {cap}{suffix}")
    if status == "SKIPPED":
        parts.append("already ingested")
    return " | ".join(parts)


def _jinja_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html", "j2"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters.update(
        pct=fmt_pct, price=fmt_price, usd=compact_usd, num=fmt_int, delta_color=delta_color
    )
    return env


def render_html(pipeline_result: dict, insights: dict | None, images: dict[str, str]) -> str:
    """Render the HTML body. `images` maps chart name -> img src (cid: or path)."""
    template = _jinja_env().get_template("report.html.j2")
    status = pipeline_result.get("status", "UNKNOWN")
    return template.render(
        title=config.REPORT_TITLE,
        result=pipeline_result,
        insights=insights,
        images=images,
        status=status,
        style=STATUS_STYLE.get(status, STATUS_STYLE["FAILED"]),
        generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        repo_url="https://github.com/ylh551400/automated-data-workflow",
    )


def render_text(pipeline_result: dict, insights: dict | None) -> str:
    """Plain-text alternative with the essentials."""
    lines = [
        f"{config.REPORT_TITLE} - {pipeline_result.get('fetch_date')}",
        f"Status: {pipeline_result.get('status')}",
        "",
    ]
    if pipeline_result.get("error_message"):
        lines += ["ERROR:", pipeline_result["error_message"], ""]
    if insights:
        lines += [
            f"Total market cap (top {insights['coins']}): {compact_usd(insights['total_market_cap'])} "
            f"({fmt_pct(insights['total_market_cap_change_pct'])} vs {insights['previous_date'] or 'n/a'})",
            f"24h volume: {compact_usd(insights['total_volume'])}",
            f"BTC: {fmt_price(insights['btc_price'])} ({fmt_pct(insights['btc_change_pct_24h'])})",
            "",
            "Top gainers:",
            *[f"  {r['symbol']:<6} {fmt_pct(r['price_change_pct_24h']):>9}" for r in insights["top_gainers"]],
            "Top losers:",
            *[f"  {r['symbol']:<6} {fmt_pct(r['price_change_pct_24h']):>9}" for r in insights["top_losers"]],
            "",
        ]
        if insights["alerts"]:
            lines += ["Alerts:", *[f"  - {a}" for a in insights["alerts"]], ""]
    q = pipeline_result.get("quality_metrics") or {}
    if q:
        lines += [
            "Data quality:",
            f"  raw={q.get('raw_records')} clean={q.get('clean_records')} "
            f"price={q.get('invalid_price')} cap={q.get('invalid_market_cap')} rank={q.get('invalid_rank')} "
            f"change={q.get('invalid_change')} stale={q.get('stale_records')} dup={q.get('duplicates_removed')}",
        ]
    for w in pipeline_result.get("warnings", []):
        lines.append(f"WARNING: {w}")
    return "\n".join(lines)


# ------------------------------------------------------------------
# Assembly + delivery
# ------------------------------------------------------------------
def build_email(
    pipeline_result: dict,
    insights: dict | None,
    chart_paths: dict[str, Path],
    sender: str,
    recipients: list[str],
) -> EmailMessage:
    """Multipart email: text/plain + (text/html with related inline images)."""
    cids = {name: make_msgid(domain="automated-data-workflow") for name in chart_paths}
    images = {name: f"cid:{cid[1:-1]}" for name, cid in cids.items()}

    msg = EmailMessage()
    msg["Subject"] = build_subject(pipeline_result, insights)
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg.set_content(render_text(pipeline_result, insights))
    msg.add_alternative(render_html(pipeline_result, insights, images), subtype="html")

    html_part = msg.get_payload()[-1]
    for name, path in chart_paths.items():
        html_part.add_related(
            Path(path).read_bytes(), maintype="image", subtype="png", cid=cids[name],
            filename=f"{name}.png",
        )
    return msg


def send_email(msg: EmailMessage) -> bool:
    """Deliver via SMTP with STARTTLS. Returns True on success."""
    try:
        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT, timeout=60) as server:
            server.ehlo()
            server.starttls()
            server.login(config.SMTP_USER, config.SMTP_PASSWORD)
            server.send_message(msg)
        logger.info("Email sent to %s", msg["To"])
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(
            "SMTP authentication failed - check SMTP_USER / SMTP_PASSWORD (Gmail needs an App Password)"
        )
    except smtplib.SMTPException as exc:
        logger.error("SMTP error: %s", exc)
    except OSError as exc:
        logger.error("Could not reach SMTP server %s:%s - %s", config.SMTP_HOST, config.SMTP_PORT, exc)
    return False


def write_preview(pipeline_result: dict, insights: dict | None, chart_paths: dict[str, Path]) -> Path:
    """Save a browser-viewable copy of the report (images referenced by relative path)."""
    config.REPORT_DIR.mkdir(parents=True, exist_ok=True)
    preview = config.REPORT_DIR / "latest_report.html"
    images = {}
    for name, path in chart_paths.items():
        try:
            images[name] = Path(os.path.relpath(path, preview.parent)).as_posix()
        except ValueError:  # different drive on Windows
            images[name] = Path(path).resolve().as_uri()
    preview.write_text(render_html(pipeline_result, insights, images), encoding="utf-8")
    logger.info("Report preview written to %s", preview)
    return preview


def send_report(
    pipeline_result: dict,
    insights: dict | None = None,
    chart_paths: dict[str, Path] | None = None,
    dry_run: bool = False,
) -> dict:
    """
    Build the report, write the preview, and send it unless dry_run or SMTP
    is not configured. Returns {"sent", "subject", "preview", "skipped_reason"}.
    """
    chart_paths = chart_paths or {}
    subject = build_subject(pipeline_result, insights)
    preview = write_preview(pipeline_result, insights, chart_paths)
    outcome = {"sent": False, "subject": subject, "preview": str(preview), "skipped_reason": None}

    logger.info("Report subject: %s", subject)
    if dry_run:
        outcome["skipped_reason"] = "dry-run"
        logger.info("Dry run - email not sent")
        return outcome
    if not config.email_configured():
        outcome["skipped_reason"] = "smtp-not-configured"
        logger.warning("SMTP_USER / SMTP_PASSWORD / REPORT_TO not set - email not sent (preview only)")
        return outcome

    msg = build_email(pipeline_result, insights, chart_paths, config.REPORT_FROM, config.REPORT_TO)
    outcome["sent"] = send_email(msg)
    return outcome
