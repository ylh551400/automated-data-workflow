"""
Dashboard Visualization Script
- Generates summary charts from pipeline data
- Includes data validation before plotting
- Saves charts as image files for embedding in reports
"""

import pandas as pd
import matplotlib.pyplot as plt
import sqlite3
import logging
import sys
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================
DB_PATH = "sales_data.db"
TABLE_NAME = "daily_sales"
OUTPUT_DIR = "charts"  # Directory to save chart images

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================
# DATA LOADING
# ============================================================
def load_data() -> pd.DataFrame:
    """
    Load data from SQLite database with validation.
    
    Returns:
        pd.DataFrame or None if loading fails
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Check if table exists
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{TABLE_NAME}'")
        if not cursor.fetchone():
            logger.error(f"Table '{TABLE_NAME}' does not exist in database")
            conn.close()
            return None
        
        df = pd.read_sql_query(f"SELECT * FROM {TABLE_NAME}", conn)
        conn.close()
        
        if df.empty:
            logger.warning("Database table is empty - no data to visualize")
            return None
        
        logger.info(f"Loaded {len(df)} records from database")
        return df
        
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return None


# ============================================================
# CHART GENERATION
# ============================================================
def create_price_by_category_chart(df: pd.DataFrame, save_path: str = None) -> bool:
    """
    Create bar chart showing average price by category.
    
    Args:
        df: DataFrame with product data
        save_path: If provided, saves chart to file instead of displaying
    
    Returns:
        bool: True if chart created successfully
    """
    try:
        # Validate required column exists
        if 'category' not in df.columns or 'price' not in df.columns:
            logger.error("Missing required columns: 'category' or 'price'")
            return False
        
        # Calculate category averages
        category_avg = df.groupby('category')['price'].mean().sort_values()
        
        if category_avg.empty:
            logger.warning("No category data available for chart")
            return False
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6))
        bars = category_avg.plot(kind='barh', ax=ax, color='steelblue', edgecolor='black')
        
        # Formatting
        ax.set_title("Average Product Price by Category", fontsize=14, fontweight='bold')
        ax.set_xlabel("Price ($)", fontsize=12)
        ax.set_ylabel("Category", fontsize=12)
        ax.grid(axis='x', alpha=0.3)
        
        # Add value labels on bars
        for i, (value, name) in enumerate(zip(category_avg.values, category_avg.index)):
            ax.text(value + 1, i, f'${value:.2f}', va='center', fontsize=10)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"Chart saved to {save_path}")
            plt.close()
        else:
            plt.show()
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create price chart: {e}")
        return False


def create_rating_distribution_chart(df: pd.DataFrame, save_path: str = None) -> bool:
    """
    Create pie chart showing rating count distribution by category.
    
    Args:
        df: DataFrame with product data
        save_path: If provided, saves chart to file instead of displaying
    
    Returns:
        bool: True if chart created successfully
    """
    try:
        # Validate required column exists
        if 'category' not in df.columns or 'rating.count' not in df.columns:
            logger.error("Missing required columns: 'category' or 'rating.count'")
            return False
        
        # Calculate rating totals by category
        rating_count = df.groupby('category')['rating.count'].sum()
        
        if rating_count.empty or rating_count.sum() == 0:
            logger.warning("No rating data available for chart")
            return False
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 8))
        colors = plt.cm.Pastel1(range(len(rating_count)))
        
        wedges, texts, autotexts = ax.pie(
            rating_count,
            labels=rating_count.index,
            autopct='%1.1f%%',
            colors=colors,
            explode=[0.02] * len(rating_count),
            shadow=True
        )
        
        # Formatting
        ax.set_title("Total Ratings Distribution by Category", fontsize=14, fontweight='bold')
        plt.setp(autotexts, fontsize=10, fontweight='bold')
        
        # Add legend with actual values
        legend_labels = [f'{cat}: {val:,.0f} ratings' for cat, val in rating_count.items()]
        ax.legend(wedges, legend_labels, title="Categories", loc="center left", bbox_to_anchor=(1, 0.5))
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"Chart saved to {save_path}")
            plt.close()
        else:
            plt.show()
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create rating chart: {e}")
        return False


def create_daily_trend_chart(df: pd.DataFrame, save_path: str = None) -> bool:
    """
    Create line chart showing record count trend over time.
    
    Args:
        df: DataFrame with product data
        save_path: If provided, saves chart to file instead of displaying
    
    Returns:
        bool: True if chart created successfully
    """
    try:
        # Validate required column exists
        if 'fetch_date' not in df.columns:
            logger.error("Missing required column: 'fetch_date'")
            return False
        
        # Calculate daily counts
        daily_counts = df.groupby('fetch_date').size()
        
        if len(daily_counts) < 2:
            logger.info("Not enough data points for trend chart (need at least 2 days)")
            return False
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 6))
        daily_counts.plot(kind='line', ax=ax, marker='o', linewidth=2, markersize=8, color='forestgreen')
        
        # Formatting
        ax.set_title("Daily Data Ingestion Trend", fontsize=14, fontweight='bold')
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Records Ingested", fontsize=12)
        ax.grid(True, alpha=0.3)
        
        # Rotate x-axis labels for readability
        plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            logger.info(f"Chart saved to {save_path}")
            plt.close()
        else:
            plt.show()
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create trend chart: {e}")
        return False


# ============================================================
# SUMMARY STATISTICS
# ============================================================
def print_summary_stats(df: pd.DataFrame) -> None:
    """
    Print summary statistics to console.
    """
    print("\n" + "=" * 50)
    print("DATA SUMMARY STATISTICS")
    print("=" * 50)
    
    print(f"\nTotal records: {len(df)}")
    print(f"Date range: {df['fetch_date'].min()} to {df['fetch_date'].max()}")
    print(f"Unique categories: {df['category'].nunique()}")
    
    print("\nPrice Statistics:")
    print(f"  Min: ${df['price'].min():.2f}")
    print(f"  Max: ${df['price'].max():.2f}")
    print(f"  Mean: ${df['price'].mean():.2f}")
    print(f"  Median: ${df['price'].median():.2f}")
    
    print("\nRecords by Category:")
    category_counts = df['category'].value_counts()
    for cat, count in category_counts.items():
        print(f"  {cat}: {count}")
    
    print("=" * 50 + "\n")


# ============================================================
# MAIN FUNCTION
# ============================================================
def generate_dashboard(save_charts: bool = False) -> bool:
    """
    Main entry point - generates all dashboard visualizations.
    
    Args:
        save_charts: If True, saves charts as image files; otherwise displays interactively
    
    Returns:
        bool: True if all charts generated successfully
    """
    logger.info("Starting dashboard generation...")
    
    # Load data
    df = load_data()
    if df is None:
        logger.error("Cannot generate dashboard - no data available")
        return False
    
    # Print summary
    print_summary_stats(df)
    
    # Create output directory if saving
    if save_charts:
        import os
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d")
        
        price_path = f"{OUTPUT_DIR}/price_by_category_{timestamp}.png"
        rating_path = f"{OUTPUT_DIR}/rating_distribution_{timestamp}.png"
        trend_path = f"{OUTPUT_DIR}/daily_trend_{timestamp}.png"
    else:
        price_path = rating_path = trend_path = None
    
    # Generate charts
    results = []
    results.append(create_price_by_category_chart(df, price_path))
    results.append(create_rating_distribution_chart(df, rating_path))
    results.append(create_daily_trend_chart(df, trend_path))
    
    success_count = sum(results)
    logger.info(f"Dashboard generation complete: {success_count}/{len(results)} charts created")
    
    return all(results)


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    # Parse command line argument for save mode
    save_mode = "--save" in sys.argv
    
    if save_mode:
        print("Running in save mode - charts will be saved to files")
    else:
        print("Running in display mode - charts will open interactively")
        print("Use --save flag to save charts as images instead")
    
    success = generate_dashboard(save_charts=save_mode)
    sys.exit(0 if success else 1)
