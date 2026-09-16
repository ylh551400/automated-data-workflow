"""
Orchestrator - single entry point for the scheduler (GitHub Actions, cron, ...).

    python main.py             run pipeline, build report, send email
    python main.py --dry-run   same, but only write reports/latest_report.html
    python main.py --force     replace today's snapshot if it already exists

Exit codes: 0 success/skipped, 1 pipeline failed, 2 report delivery failed.
"""

import argparse
import logging
import sys

import config
from charts import generate_charts
from data_pipeline import connect, run_pipeline
from insights import build_insights
from send_report import send_report

logger = logging.getLogger("main")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the automated data workflow")
    parser.add_argument("--force", action="store_true", help="replace today's snapshot if present")
    parser.add_argument("--dry-run", action="store_true", help="build the report but do not send email")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    config.setup_logging()
    logger.info("=" * 60)
    logger.info("AUTOMATED DATA WORKFLOW - STARTING (force=%s, dry_run=%s)", args.force, args.dry_run)
    logger.info("=" * 60)

    # Step 1: ETL
    logger.info("Step 1/3: data pipeline")
    result = run_pipeline(force=args.force)

    # Step 2: insights + charts (only meaningful when data exists)
    insights, charts = None, {}
    if result["status"] != "FAILED":
        logger.info("Step 2/3: insights and charts")
        try:
            conn = connect()
            try:
                insights = build_insights(conn, result["fetch_date"])
            finally:
                conn.close()
            charts = generate_charts(insights, config.CHART_DIR)
        except Exception as exc:  # noqa: BLE001 - report must still go out
            logger.exception("Insights/charts failed: %s", exc)
            result["warnings"].append(f"Insights or charts could not be generated: {exc}")

    # Step 3: report (always, so failures are visible in the inbox)
    logger.info("Step 3/3: report")
    try:
        outcome = send_report(result, insights, charts, dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Report generation failed: %s", exc)
        return 2

    logger.info("=" * 60)
    logger.info("WORKFLOW COMPLETE - status=%s email_sent=%s preview=%s",
                result["status"], outcome["sent"], outcome["preview"])
    logger.info("=" * 60)

    if result["status"] == "FAILED":
        return 1
    if not outcome["sent"] and outcome["skipped_reason"] is None:
        return 2  # SMTP configured but delivery failed
    return 0


if __name__ == "__main__":
    sys.exit(main())
