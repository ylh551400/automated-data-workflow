"""
Report Notification Script
- Sends daily summary email with pipeline metrics
- Differentiates between success/warning/failure states
- Includes actionable information for operations team
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import sqlite3
import logging
import sys

# ============================================================
# CONFIGURATION
# ============================================================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "your_email@gmail.com"
SENDER_PASSWORD = "your_app_password"  # Use App Password, not regular password
RECIPIENT_EMAILS = ["recipient_email@gmail.com"]  # Can be a list

DB_PATH = "sales_data.db"
TABLE_NAME = "daily_sales"

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================
# DATABASE METRICS COLLECTION
# ============================================================
def get_database_metrics() -> dict:
    """
    Query database for reporting metrics.
    Returns summary stats for email content.
    """
    metrics = {
        'total_records': 0,
        'today_records': 0,
        'categories': [],
        'avg_price': 0.0,
        'date_range': {'min': 'N/A', 'max': 'N/A'}
    }
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'")
        if not cursor.fetchone():
            logger.warning("Database table does not exist yet")
            return metrics
        
        # Total record count
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        metrics['total_records'] = cursor.fetchone()[0]
        
        # Today's record count
        today = datetime.now().strftime("%Y-%m-%d")
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE fetch_date = ?", (today,))
        metrics['today_records'] = cursor.fetchone()[0]
        
        # Distinct categories
        cursor.execute(f"SELECT DISTINCT category FROM {TABLE_NAME}")
        metrics['categories'] = [row[0] for row in cursor.fetchall()]
        
        # Average price
        cursor.execute(f"SELECT AVG(price) FROM {TABLE_NAME}")
        avg = cursor.fetchone()[0]
        metrics['avg_price'] = round(avg, 2) if avg else 0.0
        
        # Date range
        cursor.execute(f"SELECT MIN(fetch_date), MAX(fetch_date) FROM {TABLE_NAME}")
        row = cursor.fetchone()
        metrics['date_range'] = {'min': row[0] or 'N/A', 'max': row[1] or 'N/A'}
        
        conn.close()
        
    except sqlite3.Error as e:
        logger.error(f"Database query failed: {e}")
    
    return metrics


# ============================================================
# EMAIL CONTENT GENERATION
# ============================================================
def generate_email_content(pipeline_result: dict = None) -> tuple[str, str]:
    """
    Generate email subject and body based on pipeline results.
    
    Args:
        pipeline_result: dict from run_pipeline() or None for standalone report
    
    Returns:
        tuple: (subject, body)
    """
    db_metrics = get_database_metrics()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Determine status and subject line
    if pipeline_result:
        status = pipeline_result.get('status', 'UNKNOWN')
        records_stored = pipeline_result.get('records_stored', 0)
        quality = pipeline_result.get('quality_metrics', {})
    else:
        status = 'REPORT_ONLY'
        records_stored = db_metrics['today_records']
        quality = {}
    
    # Subject line with status indicator
    if status == 'SUCCESS':
        subject = f"✅ Daily Pipeline SUCCESS | {records_stored} records | {timestamp[:10]}"
    elif status == 'SKIPPED':
        subject = f"⚠️ Daily Pipeline SKIPPED | Already ingested | {timestamp[:10]}"
    elif status == 'FAILED':
        subject = f"🚨 ALERT: Pipeline FAILED | Action Required | {timestamp[:10]}"
    else:
        subject = f"📊 Daily Data Report | {timestamp[:10]}"
    
    # Build email body
    body_lines = [
        "=" * 50,
        "AUTOMATED DATA PIPELINE REPORT",
        "=" * 50,
        "",
        f"Timestamp: {timestamp}",
        f"Status: {status}",
        "",
        "-" * 30,
        "TODAY'S INGESTION",
        "-" * 30,
    ]
    
    if pipeline_result:
        body_lines.extend([
            f"Records fetched from API: {pipeline_result.get('records_fetched', 'N/A')}",
            f"Records stored to DB: {records_stored}",
        ])
        
        if quality:
            body_lines.extend([
                "",
                "Data Quality Summary:",
                f"  - Raw records: {quality.get('raw_records', 'N/A')}",
                f"  - Invalid price filtered: {quality.get('invalid_price', 0)}",
                f"  - Invalid category filtered: {quality.get('invalid_category', 0)}",
                f"  - Invalid rating filtered: {quality.get('invalid_rating', 0)}",
                f"  - Duplicates removed: {quality.get('duplicates_removed', 0)}",
                f"  - Clean records: {quality.get('clean_records', 'N/A')}",
            ])
        
        if pipeline_result.get('error_message'):
            body_lines.extend([
                "",
                "⚠️ ERROR DETAILS:",
                pipeline_result['error_message'],
            ])
    
    body_lines.extend([
        "",
        "-" * 30,
        "DATABASE SUMMARY",
        "-" * 30,
        f"Total records in DB: {db_metrics['total_records']}",
        f"Records added today: {db_metrics['today_records']}",
        f"Average price: ${db_metrics['avg_price']}",
        f"Categories: {', '.join(db_metrics['categories']) if db_metrics['categories'] else 'N/A'}",
        f"Data range: {db_metrics['date_range']['min']} to {db_metrics['date_range']['max']}",
        "",
        "-" * 30,
        "NEXT STEPS",
        "-" * 30,
    ])
    
    # Actionable recommendations based on status
    if status == 'FAILED':
        body_lines.extend([
            "1. Check pipeline.log for detailed error messages",
            "2. Verify API endpoint is accessible",
            "3. Review network/firewall settings",
            "4. Re-run pipeline manually after fixing issues",
        ])
    elif status == 'SKIPPED':
        body_lines.extend([
            "1. Data already ingested today - no action needed",
            "2. If re-ingestion required, clear today's records first",
        ])
    elif db_metrics['today_records'] == 0:
        body_lines.extend([
            "⚠️ WARNING: No new records today",
            "1. Verify API is returning data",
            "2. Check data quality filters aren't too strict",
        ])
    else:
        body_lines.extend([
            "✓ No action required - pipeline healthy",
            "✓ Dashboard should refresh automatically",
        ])
    
    body_lines.extend([
        "",
        "=" * 50,
        "This is an automated message from the Data Pipeline.",
        "Do not reply to this email.",
        "=" * 50,
    ])
    
    return subject, "\n".join(body_lines)


# ============================================================
# EMAIL SENDING
# ============================================================
def send_email(subject: str, body: str, recipients: list = RECIPIENT_EMAILS) -> bool:
    """
    Send email via SMTP with error handling.
    
    Returns:
        bool: True if sent successfully, False otherwise
    """
    try:
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = SENDER_EMAIL
        msg['To'] = ", ".join(recipients)
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        
        logger.info(f"Email sent successfully to {recipients}")
        return True
        
    except smtplib.SMTPAuthenticationError:
        logger.error("Email authentication failed - check credentials")
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


# ============================================================
# MAIN FUNCTION
# ============================================================
def send_report(pipeline_result: dict = None) -> bool:
    """
    Main entry point - generates and sends the report email.
    
    Args:
        pipeline_result: Optional dict from data_pipeline.run_pipeline()
                        If None, generates report from current DB state
    
    Returns:
        bool: True if email sent successfully
    """
    logger.info("Generating report email...")
    
    subject, body = generate_email_content(pipeline_result)
    
    # Print to console for debugging
    print("\n" + "=" * 50)
    print("EMAIL PREVIEW")
    print("=" * 50)
    print(f"Subject: {subject}")
    print("-" * 50)
    print(body)
    print("=" * 50 + "\n")
    
    # Uncomment below to actually send email
    # return send_email(subject, body)
    
    logger.info("Email preview generated (sending disabled - uncomment to enable)")
    return True


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    # Standalone execution - report based on current DB state
    success = send_report()
    sys.exit(0 if success else 1)
