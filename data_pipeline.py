"""
Data Pipeline Script
- Fetches product data from FakeStoreAPI
- Validates and cleans data
- Stores in SQLite with deduplication check
- Returns metrics for downstream reporting
"""

import requests
import pandas as pd
from datetime import datetime
import sqlite3
import logging
import sys
import time

# ============================================================
# CONFIGURATION
# ============================================================
API_URL = "https://fakestoreapi.com/products"
DB_PATH = "sales_data.db"
TABLE_NAME = "daily_sales"

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# Data quality thresholds
REQUIRED_FIELDS = ['id', 'title', 'category', 'price', 'rating.rate', 'rating.count']
MIN_EXPECTED_RECORDS = 10  # Alert if fewer records than expected

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
# DATA INGESTION (with retry logic)
# ============================================================
def fetch_data_from_api(url: str, max_retries: int = MAX_RETRIES) -> list:
    """
    Fetch data from API with retry mechanism and timeout.
    Raises exception after all retries exhausted.
    """
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"API fetch attempt {attempt}/{max_retries}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data)} records from API")
            return data
            
        except requests.exceptions.Timeout:
            logger.warning(f"Attempt {attempt}: Request timed out")
        except requests.exceptions.HTTPError as e:
            logger.warning(f"Attempt {attempt}: HTTP error {e.response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt}: Request failed - {str(e)}")
        except ValueError as e:
            logger.error(f"Attempt {attempt}: Invalid JSON response - {str(e)}")
            raise  # JSON parse error is not recoverable via retry
        
        if attempt < max_retries:
            logger.info(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
            time.sleep(RETRY_DELAY_SECONDS)
    
    raise RuntimeError(f"API fetch failed after {max_retries} attempts")


# ============================================================
# SCHEMA VALIDATION
# ============================================================
def validate_schema(data: list, required_fields: list) -> bool:
    """
    Check if API response contains all required fields.
    Returns False if schema has changed (fields missing).
    """
    if not data:
        logger.error("Schema validation failed: Empty response")
        return False
    
    # Flatten first record to check nested fields like 'rating.rate'
    sample_df = pd.json_normalize(data[0:1])
    actual_fields = set(sample_df.columns)
    required_set = set(required_fields)
    
    missing_fields = required_set - actual_fields
    if missing_fields:
        logger.error(f"Schema validation failed: Missing fields {missing_fields}")
        return False
    
    logger.info("Schema validation passed")
    return True


# ============================================================
# DATA QUALITY CHECKS & CLEANING
# ============================================================
def clean_and_validate_data(data: list) -> tuple[pd.DataFrame, dict]:
    """
    Transform raw API data, apply quality filters, return clean df and metrics.
    
    Quality rules:
    - price must be positive
    - category must not be empty
    - rating.rate must be between 0 and 5
    - id must be unique within batch
    
    Returns:
        tuple: (cleaned_dataframe, metrics_dict)
    """
    df = pd.json_normalize(data)
    df = df[REQUIRED_FIELDS]
    df['fetch_date'] = datetime.now().strftime("%Y-%m-%d")
    
    original_count = len(df)
    metrics = {
        'raw_records': original_count,
        'invalid_price': 0,
        'invalid_category': 0,
        'invalid_rating': 0,
        'duplicates_removed': 0,
        'clean_records': 0
    }
    
    # Filter 1: Price must be positive
    invalid_price_mask = (df['price'] <= 0) | (df['price'].isna())
    metrics['invalid_price'] = invalid_price_mask.sum()
    df = df[~invalid_price_mask]
    
    # Filter 2: Category must not be empty
    invalid_category_mask = (df['category'].isna()) | (df['category'].str.strip() == '')
    metrics['invalid_category'] = invalid_category_mask.sum()
    df = df[~invalid_category_mask]
    
    # Filter 3: Rating must be valid (0-5 range)
    invalid_rating_mask = (df['rating.rate'] < 0) | (df['rating.rate'] > 5) | (df['rating.rate'].isna())
    metrics['invalid_rating'] = invalid_rating_mask.sum()
    df = df[~invalid_rating_mask]
    
    # Filter 4: Remove duplicates by id within this batch
    duplicates = df.duplicated(subset=['id'], keep='first').sum()
    metrics['duplicates_removed'] = duplicates
    df = df.drop_duplicates(subset=['id'], keep='first')
    
    metrics['clean_records'] = len(df)
    
    # Log summary
    filtered_count = original_count - metrics['clean_records']
    logger.info(f"Data cleaning complete: {metrics['clean_records']}/{original_count} records passed")
    if filtered_count > 0:
        logger.warning(f"Filtered out {filtered_count} invalid records: "
                      f"price={metrics['invalid_price']}, "
                      f"category={metrics['invalid_category']}, "
                      f"rating={metrics['invalid_rating']}, "
                      f"duplicates={metrics['duplicates_removed']}")
    
    return df, metrics


# ============================================================
# IDEMPOTENCY CHECK (prevent duplicate daily runs)
# ============================================================
def check_already_ingested_today(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Check if data for today's date already exists in the database.
    Prevents duplicate ingestion from accidental double-triggers.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    cursor = conn.cursor()
    
    # Check if table exists first
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        return False  # Table doesn't exist yet, safe to proceed
    
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE fetch_date = ?", (today,))
    count = cursor.fetchone()[0]
    
    if count > 0:
        logger.warning(f"Idempotency check: {count} records already exist for {today}")
        return True
    return False


# ============================================================
# DATA STORAGE
# ============================================================
def store_data(df: pd.DataFrame, db_path: str, table_name: str) -> int:
    """
    Store cleaned data to SQLite database.
    Returns number of records written.
    """
    conn = sqlite3.connect(db_path)
    
    try:
        # Idempotency check
        if check_already_ingested_today(conn, table_name):
            logger.warning("Skipping storage: Data already ingested today. "
                         "Set FORCE_OVERWRITE=True to override.")
            return 0
        
        df.to_sql(table_name, conn, if_exists='append', index=False)
        logger.info(f"Successfully stored {len(df)} records to {db_path}")
        return len(df)
        
    finally:
        conn.close()


# ============================================================
# MAIN PIPELINE
# ============================================================
def run_pipeline() -> dict:
    """
    Execute full ETL pipeline and return execution metrics.
    
    Returns:
        dict: Pipeline execution metrics for reporting
    """
    logger.info("=" * 50)
    logger.info("STARTING DATA PIPELINE")
    logger.info("=" * 50)
    
    result = {
        'status': 'FAILED',
        'timestamp': datetime.now().isoformat(),
        'records_fetched': 0,
        'records_stored': 0,
        'quality_metrics': {},
        'error_message': None
    }
    
    try:
        # Step 1: Fetch data
        raw_data = fetch_data_from_api(API_URL)
        result['records_fetched'] = len(raw_data)
        
        # Step 2: Validate schema (detect API changes)
        if not validate_schema(raw_data, REQUIRED_FIELDS):
            raise ValueError("Schema validation failed - API structure may have changed")
        
        # Step 3: Clean and validate data quality
        clean_df, quality_metrics = clean_and_validate_data(raw_data)
        result['quality_metrics'] = quality_metrics
        
        # Step 4: Alert if record count is suspiciously low
        if len(clean_df) < MIN_EXPECTED_RECORDS:
            logger.warning(f"ALERT: Only {len(clean_df)} clean records, expected >= {MIN_EXPECTED_RECORDS}")
        
        # Step 5: Store to database
        records_stored = store_data(clean_df, DB_PATH, TABLE_NAME)
        result['records_stored'] = records_stored
        
        # Final status
        if records_stored > 0:
            result['status'] = 'SUCCESS'
            logger.info(f"Pipeline completed successfully: {records_stored} records stored")
        else:
            result['status'] = 'SKIPPED'
            logger.info("Pipeline completed: No new records stored (already ingested today)")
        
    except Exception as e:
        result['status'] = 'FAILED'
        result['error_message'] = str(e)
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    
    finally:
        logger.info("=" * 50)
        logger.info(f"PIPELINE FINISHED - Status: {result['status']}")
        logger.info("=" * 50)
    
    return result


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    try:
        metrics = run_pipeline()
        print(f"\nPipeline Result: {metrics['status']}")
        print(f"Records: {metrics['records_stored']} stored / {metrics['records_fetched']} fetched")
    except Exception as e:
        print(f"\nPipeline Error: {e}")
        sys.exit(1)
