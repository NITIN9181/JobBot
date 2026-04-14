from __future__ import annotations
import pandas as pd
import os
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from modules.utils import retry

# Google Sheets Integration Imports
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

"""
GOOGLE SHEETS SETUP INSTRUCTIONS:
1. Create a Google Cloud Project: https://console.cloud.google.com/
2. Enable APIs: Search for and enable "Google Sheets API" and "Google Drive API".
3. Create a Service Account: 
   - Go to "APIs & Services" > "Credentials" > "Create Credentials" > "Service Account".
4. Generate Key:
   - Go to the "Keys" tab in the service account settings.
   - Click "Add Key" > "Create new key" > "JSON".
   - Save the downloaded file as 'credentials.json' in your project root.
5. Create a Google Sheet and share it with the service account email.
6. Update .env with GOOGLE_SHEETS_CRED_FILE and GOOGLE_SHEET_NAME.
"""

@retry(max_attempts=3, delay=5)
def setup_google_sheets(sheet_name: str) -> tuple[Optional[gspread.Spreadsheet], Optional[gspread.Worksheet]]:
    """
    Connects to Google Sheets and ensures the 'Job Listings' worksheet exists.
    """
    if not GSHEETS_AVAILABLE:
        return None, None

    cred_file = os.getenv("GOOGLE_SHEETS_CRED_FILE", "credentials.json")
    if not os.path.exists(cred_file):
        logger.warning(f"Google credentials file '{cred_file}' not found.")
        return None, None

    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(sheet_name)
        
        try:
            worksheet = spreadsheet.worksheet("Job Listings")
        except gspread.WorksheetNotFound:
            logger.info("Worksheet 'Job Listings' not found. Creating it now...")
            worksheet = spreadsheet.add_worksheet(title="Job Listings", rows="1000", cols="20")
            
        return spreadsheet, worksheet
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}")
        raise # Rethrow for retry decorator

@retry(max_attempts=3, delay=2)
def check_sheet_duplicates(worksheet, job_url: str) -> bool:
    """Checks if a job URL already exists in the 'Job URL' column (Col 10)."""
    try:
        urls = worksheet.col_values(10)
        return job_url in urls
    except Exception as e:
        logger.error(f"Error checking duplicates in sheet: {e}")
        raise

@retry(max_attempts=3, delay=5)
def update_sheet_formatting(worksheet):
    """Applies conditional formatting and freezes headers."""
    try:
        # Format Header (Row 1): Bold, Background Color
        worksheet.format("A1:L1", {
            "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8},
            "textFormat": {"bold": True}
        })
        worksheet.freeze(rows=1)
        
        # Add conditional formatting for AI Score (Col 8)
        requests = [
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": 1000, "startColumnIndex": 7, "endColumnIndex": 8}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_GREATER_EQUAL", "values": [{"userEnteredValue": "80"}]},
                            "format": {"backgroundColor": {"red": 0.7, "green": 0.9, "blue": 0.7}} # Green
                        }
                    },
                    "index": 0
                }
            }
        ]
        worksheet.spreadsheet.batch_update({"requests": requests})
        logger.info("Applied formatting to Google Sheet.")
    except Exception as e:
        logger.warning(f"Could not apply formatting: {e}")
        raise

@retry(max_attempts=3, delay=5)
def get_application_stats(worksheet) -> dict:
    """Reads the Status column and returns counts."""
    stats = {"total": 0, "not_applied": 0, "applied": 0, "interview": 0, "rejected": 0, "offer": 0}
    try:
        statuses = worksheet.col_values(11)[1:] # Status is column 11
        stats["total"] = len(statuses)
        for s in statuses:
            s_low = str(s).lower().replace(" ", "_").strip()
            if s_low in stats:
                stats[s_low] += 1
            else:
                stats["not_applied"] += 1
        return stats
    except Exception as e:
        logger.error(f"Error getting sheet stats: {e}")
        raise

def display_application_stats(stats: dict):
    """Prints a premium ASCII box with application statistics."""
    total = stats.get("total", 0)
    applied = stats.get("applied", 0)
    interview = stats.get("interview", 0)
    response_rate = (interview / applied * 100) if applied > 0 else 0
    
    print("\n" + "═"*40)
    print(f"║ {'TRACKER STATISTICS':^36} ║")
    print("═"*40)
    print(f"║ Total Jobs:          {total:<17} ║")
    print(f"║ Applied:             {applied:<17} ║")
    print(f"║ Interviewing:        {interview:<17} ║")
    print(f"║ Response Rate:       {response_rate:>6.1f}%           ║")
    print("═"*40 + "\n")

def export_to_google_sheets(df: pd.DataFrame, config: dict) -> Dict[str, Any]:
    """Appends new jobs to Google Sheets."""
    status = {"success": False, "url": "", "count": 0}
    if df.empty: return status

    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "JobBot_Output")
    spreadsheet, worksheet = setup_google_sheets(sheet_name)
    if not spreadsheet or not worksheet:
        return status
    
    status["url"] = spreadsheet.url
    try:
        # Ensure headers
        existing_values = worksheet.get_all_values()
        if not existing_values:
            headers = ["Date Found", "Title", "Company", "Location", "Salary Range", "Job Type", "Skills Matched", "AI Score", "AI Reason", "Job URL", "Status", "Notes"]
            worksheet.append_row(headers)

        existing_urls = worksheet.col_values(10)
        new_rows = []
        current_date = datetime.now().strftime("%Y-%m-%d")

        for _, row in df.iterrows():
            job_url = str(row.get('job_url', ''))
            if job_url in existing_urls: continue
            
            salary_str = f"{row.get('min_amount', 'N/A')} - {row.get('max_amount', 'N/A')}"
            matched_skills = ", ".join(row.get('matched_skills', [])) if isinstance(row.get('matched_skills'), list) else str(row.get('matched_skills', 'N/A'))

            # Sanitize row data for JSON compliance (Google Sheets API)
            row_data = [
                current_date, 
                str(row.get('title', 'N/A')), 
                str(row.get('company', 'N/A')),
                str(row.get('location', 'N/A')), 
                salary_str, 
                str(row.get('job_type', 'N/A')),
                matched_skills, 
                row.get('ai_match_score', 0) if pd.notnull(row.get('ai_match_score')) else 0, 
                str(row.get('ai_match_reason', 'N/A')) if pd.notnull(row.get('ai_match_reason')) else "N/A",
                job_url, 
                "Not Applied", 
                ""
            ]
            
            # Final safety check for NaN elements
            row_data = [("" if pd.isna(item) else item) for item in row_data]
            new_rows.append(row_data)

        if new_rows:
            @retry(max_attempts=3, delay=10)
            def do_append():
                worksheet.append_rows(new_rows)
            
            do_append()
            logger.info(f"Appended {len(new_rows)} new jobs to Google Sheets.")
            status["success"] = True
            status["count"] = len(new_rows)
            update_sheet_formatting(worksheet)
        else:
            logger.info("No new jobs to add to Google Sheets.")
            status["success"] = True

    except Exception as e:
        logger.error(f"Error in export_to_google_sheets: {e}")
        status["success"] = False
        
    return status

def export_to_csv(df: pd.DataFrame, output_dir: str = "output") -> str:
    """Exports to a timestamped CSV."""
    if df.empty: return ""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filepath = os.path.join(output_dir, f"jobs_{timestamp}.csv")
    
    cols = ["title", "company", "location", "job_url", "job_type", "min_amount", "max_amount", "currency", "date_posted", "description", "source_board", "matched_skills", "ai_match_score", "ai_match_reason"]
    existing_cols = [c for c in cols if c in df.columns]
    
    try:
        df[existing_cols].to_csv(filepath, index=False)
        logger.info(f"Exported {len(df)} jobs to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to export CSV: {e}")
        return ""

def export_latest_csv(df: pd.DataFrame, output_dir: str = "output") -> str:
    """Exports both timestamped and latest_jobs.csv."""
    filepath = export_to_csv(df, output_dir)
    if filepath and not df.empty:
        latest_path = os.path.join(output_dir, "latest_jobs.csv")
        try:
            df.to_csv(latest_path, index=False)
            logger.info(f"Updated latest results at {latest_path}")
        except Exception as e:
            logger.error(f"Failed to update latest CSV: {e}")
    return filepath

def display_terminal_summary(df: pd.DataFrame, top_n: int = 5):
    """Prints a clean summary to the terminal."""
    print(f"\n{'='*50}\n{'RUN SUMMARY':^50}\n{'='*50}")
    print(f"Total jobs: {len(df)}")
    
    if df.empty:
        print("\nNo jobs found.")
        return

    is_scored = "ai_match_score" in df.columns
    sort_col = "ai_match_score" if is_scored else "skill_match_count"
    display_df = df.sort_values(by=sort_col, ascending=False).head(top_n)

    print("\nTop Matches:")
    for i, (_, row) in enumerate(display_df.iterrows(), 1):
        score = f"{int(row.get(sort_col, 0))}%" if is_scored else "N/A"
        print(f"[{i}] {score} - {row['title']} @ {row['company']}")

def generate_run_summary(scraped: int, matched: int, new: int, time_sec: float, ai_stats: dict = None, gs_status: dict = None) -> str:
    """Generates a text summary of the run."""
    summary = f"""
---------------------------------------
JobBot Run Results
---------------------------------------
Scraped: {scraped} | Matched: {matched} | New: {new}
Time:    {time_sec:.1f}s
"""
    if ai_stats:
        summary += f"AI Match: Top Score {ai_stats.get('top_score', 0)}% ({ai_stats.get('top_job', 'N/A')})\n"
    if gs_status:
        summary += f"Sheets:  {'[OK]' if gs_status.get('success') else '[FAIL]'} {gs_status.get('count', 0)} added\n"
    
    return summary.strip()

if __name__ == "__main__":
    from modules.logger_setup import setup_logging
    setup_logging()
    logger.info("Exporter test session.")
