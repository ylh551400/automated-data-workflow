# automated-data-workflow
End-to-end automated data analysis pipeline using Python &amp; Make
 

This project demonstrates a **fully automated data pipeline** that performs end-to-end data analysis — from data ingestion to visualization and reporting — using Python and low-code workflow automation tools such as **Make** (Integromat) and **Zapier**.

---

##  Project Overview

The workflow automatically:
1. Pulls product data from a public API (`FakeStoreAPI`)
2. Cleans and stores the data in a local SQL database
3. Visualizes sales insights in a dashboard (Power BI / Jupyter)
4. Sends a daily summary email automatically

This simulates a real-world **ETL + BI reporting pipeline**, with full automation between data collection, transformation, and delivery.

---

##  System Architecture

[SCHEDULER]
↓
[HTTP GET: FakeStore API]
↓
[Python Script: data_pipeline.py → SQLite DB]
↓
[Dashboard Refresh (Power BI / Jupyter)]
↓
[send_report.py → Gmail Notification]


Automation built with **Make** or **Zapier**:
- **Trigger:** Daily scheduler (runs every morning)
- **Action 1:** HTTP GET request to fetch new data
- **Action 2:** Execute Python data cleaning script
- **Action 3:** Update database or Google Sheet
- **Action 4:** Send email or Slack summary notification

---

##  Tech Stack

| Layer | Tools / Technologies |
|-------|-----------------------|
| **Data Source** | [FakeStoreAPI](https://fakestoreapi.com/products) |
| **Processing** | Python (pandas, requests, sqlite3) |
| **Storage** | SQLite (or Google Sheets / Airtable) |
| **Automation** | Make / Zapier (low-code connectors) |
| **Visualization** | Power BI / Jupyter Notebook |
| **Reporting** | reportlab, smtplib (email) |

---

##  Setup Instructions

### 1️ Clone the repo
 
git clone https://github.com/yourusername/automated-data-workflow.git
cd automated-data-workflow

### 2. Install dependencies
pip install -r requirements.txt

### 3. Run the pipeline manually (for testing)
python data_pipeline.py


### 4. Optional: Send a test email
python send_report.py

## Design Logic
| Step                  | Objective                              | Implementation                                       |
| --------------------- | -------------------------------------- | ---------------------------------------------------- |
| **1. Data Ingestion** | Collect daily product & pricing data   | HTTP GET request via `requests`                      |
| **2. Data Cleaning**  | Select essential fields, add timestamp | `pandas` dataframe transformation                    |
| **3. Storage**        | Maintain daily data history            | `sqlite3` database (append mode)                     |
| **4. Visualization**  | Generate category-level insights       | Power BI / Matplotlib bar charts                     |
| **5. Reporting**      | Notify team automatically              | Python `smtplib` email trigger / Zapier Gmail module |
