# Automated Data Workflow

End-to-end automated data analysis pipeline with built-in data quality monitoring, error handling, and operational alerting.

---

## Project Overview

This project demonstrates a **production-ready ETL + BI reporting pipeline** that:

1. Pulls product data from a public API (`FakeStoreAPI`)
2. Validates schema integrity and data quality
3. Stores clean data in a SQL database with deduplication
4. Generates visualizations for business insights
5. Sends automated reports with actionable metrics

Built with Python and designed for integration with low-code automation tools (Make / Zapier).

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Retry Mechanism** | 3x automatic retry with exponential backoff for API failures |
| **Schema Validation** | Detects upstream API changes before they break downstream processes |
| **Data Quality Rules** | Filters invalid records (negative prices, empty categories, out-of-range ratings) |
| **Idempotency Check** | Prevents duplicate ingestion from accidental double-triggers |
| **Metrics-Based Alerting** | Email reports include record counts, filtered records, and actionable next steps |
| **Comprehensive Logging** | All operations logged to file + console for debugging |

---

## System Architecture

```
┌─────────────┐
│  SCHEDULER  │  (Make / Zapier / Cron)
└──────┬──────┘
       ▼
┌─────────────────────────────────────────────────────────┐
│  main.py (Orchestrator)                                 │
│  ┌────────────────────────────────────────────────────┐ │
│  │  data_pipeline.py                                  │ │
│  │  ├─ HTTP GET: FakeStore API (with retry)          │ │
│  │  ├─ Schema Validation                             │ │
│  │  ├─ Data Quality Filtering                        │ │
│  │  ├─ Idempotency Check                             │ │
│  │  └─ SQLite Storage                                │ │
│  └────────────────────────────────────────────────────┘ │
│                         ▼                               │
│  ┌────────────────────────────────────────────────────┐ │
│  │  send_report.py                                    │ │
│  │  ├─ Collect DB Metrics                            │ │
│  │  ├─ Generate Status-Based Email                   │ │
│  │  └─ ✅ SUCCESS / ⚠️ SKIPPED / 🚨 FAILED           │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
       ▼
┌─────────────┐
│  dashboard  │  (Power BI / Matplotlib)
└─────────────┘
```

---

## Tech Stack

| Layer | Tools |
|-------|-------|
| **Data Source** | [FakeStoreAPI](https://fakestoreapi.com/products) |
| **Processing** | Python (pandas, requests) |
| **Storage** | SQLite |
| **Automation** | Make / Zapier (designed for integration) |
| **Visualization** | Matplotlib / Power BI |
| **Reporting** | smtplib (Gmail SMTP) |

---

## File Structure

```
automated-data-workflow/
├── main.py              # Orchestrator - runs full pipeline
├── data_pipeline.py     # ETL: fetch, validate, clean, store
├── send_report.py       # Notification with metrics
├── dashboard.py         # Visualization generation
├── requirements.txt
├── pipeline.log         # Generated at runtime
└── sales_data.db        # Generated at runtime
```

---

## Quick Start

### 1. Clone the repo
```bash
git clone https://github.com/ylh551400/automated-data-workflow.git
cd automated-data-workflow
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the full pipeline
```bash
python main.py
```

### 4. Generate dashboard (optional)
```bash
# Interactive display
python dashboard.py

# Save charts as images
python dashboard.py --save
```

---

## Data Quality Rules

The pipeline filters out records that fail these validations:

| Rule | Logic | Rationale |
|------|-------|-----------|
| Price validation | `price > 0` | Negative/zero prices indicate data errors |
| Category validation | `category IS NOT NULL AND category != ''` | Empty categories break downstream grouping |
| Rating validation | `0 <= rating <= 5` | Out-of-range ratings indicate corrupted data |
| Deduplication | Unique `id` per batch | Prevents double-counting |

Filtered records are logged but not stored, with counts included in the daily report.

---

## Email Report Example

**Subject:** `✅ Daily Pipeline SUCCESS | 20 records | 2024-01-15`

```
==================================================
AUTOMATED DATA PIPELINE REPORT
==================================================

Timestamp: 2024-01-15 09:00:15
Status: SUCCESS

------------------------------
TODAY'S INGESTION
------------------------------
Records fetched from API: 20
Records stored to DB: 20

Data Quality Summary:
  - Raw records: 20
  - Invalid price filtered: 0
  - Invalid category filtered: 0
  - Invalid rating filtered: 0
  - Duplicates removed: 0
  - Clean records: 20

------------------------------
DATABASE SUMMARY
------------------------------
Total records in DB: 140
Records added today: 20
Average price: $109.95
Categories: electronics, jewelery, men's clothing, women's clothing
Data range: 2024-01-08 to 2024-01-15

------------------------------
NEXT STEPS
------------------------------
✓ No action required - pipeline healthy
✓ Dashboard should refresh automatically
```

---

## Error Handling

| Scenario | Behavior |
|----------|----------|
| API timeout / 5xx error | Retry up to 3 times with 5s delay |
| API schema change | Halt pipeline, send alert email |
| All records filtered | Warning in logs + email, pipeline continues |
| Already ingested today | Skip storage, send "SKIPPED" status email |
| SMTP failure | Log error, exit with code 2 |

---

## Automation Integration (Make / Zapier)

This pipeline is designed to be triggered by external schedulers:

| Step | Module | Configuration |
|------|--------|---------------|
| 1 | **Scheduler** | Daily trigger (e.g., 9:00 AM) |
| 2 | **Code/SSH Module** | Execute `python main.py` |
| 3 | **Error Router** | Check exit code (0=success, 1=pipeline fail, 2=report fail) |
| 4 | **Slack/Email** | Forward alerts on non-zero exit |

---

## Monitoring Checklist (Operations Perspective)

For teams running this in production:

- [ ] **Daily:** Check email report for record counts and anomalies
- [ ] **Weekly:** Review `pipeline.log` for warning patterns
- [ ] **Monthly:** Validate data quality trends in dashboard
- [ ] **On Alert:** If email shows 0 records or FAILED status, check API availability first

---

## Future Improvements

- [ ] Migrate from SQLite to cloud database (BigQuery/Snowflake) for team collaboration
- [ ] Add Slack webhook for real-time failure alerts
- [ ] Implement data versioning for rollback capability
- [ ] Add unit tests for data validation functions

 
