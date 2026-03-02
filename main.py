"""
Main Orchestrator Script
- Runs the complete pipeline: Ingest -> Validate -> Store -> Report
- Integrates data_pipeline and send_report modules
- Provides single entry point for automation tools (Make/Zapier)
"""

import sys
import logging
from datetime import datetime

# Import pipeline modules
from data_pipeline import run_pipeline
from send_report import send_report

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# MAIN ORCHESTRATOR
# ============================================================
def main():
    """
    Execute full automated workflow:
    1. Run data pipeline (fetch, validate, store)
    2. Generate and send report with pipeline results
    
    Exit codes:
        0: Success
        1: Pipeline failed
        2: Report failed
    """
    logger.info("=" * 60)
    logger.info("AUTOMATED DATA WORKFLOW - STARTING")
    logger.info(f"Execution time: {datetime.now().isoformat()}")
    logger.info("=" * 60)
    
    pipeline_result = None
    
    # Step 1: Run data pipeline
    try:
        logger.info("Step 1/2: Running data pipeline...")
        pipeline_result = run_pipeline()
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        pipeline_result = {
            'status': 'FAILED',
            'timestamp': datetime.now().isoformat(),
            'records_fetched': 0,
            'records_stored': 0,
            'quality_metrics': {},
            'error_message': str(e)
        }
    
    # Step 2: Send report (always send, even on failure)
    try:
        logger.info("Step 2/2: Sending report notification...")
        report_sent = send_report(pipeline_result)
        
        if not report_sent:
            logger.warning("Report notification may have failed")
            
    except Exception as e:
        logger.error(f"Report sending failed: {e}")
        return 2
    
    # Final status
    logger.info("=" * 60)
    logger.info("AUTOMATED DATA WORKFLOW - COMPLETE")
    logger.info(f"Final status: {pipeline_result.get('status', 'UNKNOWN')}")
    logger.info("=" * 60)
    
    # Return appropriate exit code
    if pipeline_result.get('status') == 'FAILED':
        return 1
    return 0


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
