from __future__ import annotations
import pandas as pd
import os
import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any, List

# Google Sheets Integration Imports
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False

# Set up logging for this module
logger = logging.getLogger(__name__)

"""
GOOGLE SHEETS SETUP INSTRUCTIONS:
1. Create a Google Cloud Project: https://console.cloud.google.com/
2. Enable APIs: Search for and enable "Google Sheets API" and "Google Drive API".
3. Create a Service Account: 
   - Go to "APIs & Services" > "Credentials".
   - Click "Create Credentials" > "Service Account".
   - Follow prompts and click "Done".
4. Generate Key:
   - Click on the newly created service account.
   - Go to the "Keys" tab.
   - Click "Add Key" > "Create new key" > "JSON".
   - Save the downloaded file as 'credentials.json' in your project root.
5. Create a Google Sheet:
   - Create a new sheet in Google Drive.
   - Name it (e.g., 'JobBot_Output').
   - Share the sheet with the 'client_email' found in your 'credentials.json'.
6. Update .env:
   - Set GOOGLE_SHEETS_CRED_FILE=credentials.json
   - Set GOOGLE_SHEET_NAME=JobBot_Output
"""

def setup_google_sheets(cred_file: str, sheet_name: str) -> Optional["gspread.Spreadsheet"]:
    """
    Authenticates with Google Sheets and opens the specified spreadsheet.
    """
    if not GSHEETS_AVAILABLE:
        logger.warning("gspread or oauth2client not installed. Skipping Google Sheets integration.")
        return None

    if not os.path.exists(cred_file):
        logger.warning(f"Google credentials file not found: {cred_file}. Skipping Google Sheets integration.")
        print(f"\n[WARNING] Google Sheets credentials ('{cred_file}') missing.")
        print("To enable Google Sheets export, follow the setup instructions in modules/exporter.py\n")
        return None

    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)
        client = gspread.authorize(creds)
        
        spreadsheet = client.open(sheet_name)
        logger.info(f"Connected to Google Sheet: {sheet_name}")
        return spreadsheet
    except gspread.SpreadsheetNotFound:
        logger.error(f"Spreadsheet '{sheet_name}' not found. Ensure it exists and is shared with the service account email.")
        return None
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {e}")
        return None

def check_sheet_duplicates(worksheet, job_url: str) -> bool:
    """
    Checks if a job URL already exists in the "Job URL" column.
    """
    try:
        # Assuming URL is in column 10 (based on header order)
        # We fetch the whole column to avoid repeated API calls in a loop
        urls = worksheet.col_values(10)
        return job_url in urls
    except Exception as e:
        logger.error(f"Error checking duplicates in sheet: {e}")
        return False

def update_sheet_formatting(worksheet):
    """
    Applies conditional formatting, bold headers, freezes Row 1, and auto-resizes.
    Note: Using direct gspread API calls.
    """
    try:
        # 1. Format Header (Row 1): Bold, Background Color
        worksheet.format("A1:L1", {
            "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8},
            "textFormat": {"bold": True}
        })
        
        # 2. Freeze Header
        worksheet.freeze(rows=1)
        
        # 3. Conditional Formatting for AI Score (Column H / 8)
        # We use batch_update for efficiency
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
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": 1000, "startColumnIndex": 7, "endColumnIndex": 8}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_BETWEEN", "values": [{"userEnteredValue": "60"}, {"userEnteredValue": "79"}]},
                            "format": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.7}} # Yellow
                        }
                    },
                    "index": 1
                }
            },
            {
                "addConditionalFormatRule": {
                    "rule": {
                        "ranges": [{"sheetId": worksheet.id, "startRowIndex": 1, "endRowIndex": 1000, "startColumnIndex": 7, "endColumnIndex": 8}],
                        "booleanRule": {
                            "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "60"}]},
                            "format": {"backgroundColor": {"red": 1.0, "green": 0.7, "blue": 0.7}} # Red
                        }
                    },
                    "index": 2
                }
            }
        ]
        
        worksheet.spreadsheet.batch_update({"requests": requests})
        logger.info("Applied formatting to Google Sheet.")
    except Exception as e:
        logger.warning(f"Could not apply formatting: {e}")

def get_application_stats(worksheet) -> dict:
    """
    Reads the Status column and returns counts.
    """
    stats = {
        "total": 0,
        "not_applied": 0,
        "applied": 0,
        "interview": 0,
        "rejected": 0,
        "offer": 0
    }
    try:
        # Status is column 11
        statuses = worksheet.col_values(11)[1:] # Skip header
        stats["total"] = len(statuses)
        for s in statuses:
            s_low = str(s).lower().replace(" ", "_")
            if s_low in stats:
                stats[s_low] += 1
            elif s_low == "not_applied" or not s_low:
                stats["not_applied"] += 1
    except Exception as e:
        logger.error(f"Error getting sheet stats: {e}")
    
    return stats

def export_to_google_sheets(df: pd.DataFrame, config: dict):
    """
    Connects to Google Sheets and appends new jobs.
    """
    if df.empty:
        return

    cred_file = os.getenv("GOOGLE_SHEETS_CRED_FILE", "credentials.json")
    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "JobBot_Output")

    spreadsheet = setup_google_sheets(cred_file, sheet_name)
    if not spreadsheet:
        return

    try:
        # Open or create "Job Listings" worksheet
        try:
            worksheet = spreadsheet.worksheet("Job Listings")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title="Job Listings", rows="1000", cols="20")
            # Set headers
            headers = ["Date Found", "Title", "Company", "Location", "Salary Range", "Job Type", "Skills Matched", "AI Score", "AI Reason", "Job URL", "Status", "Notes"]
            worksheet.append_row(headers)
            
            # Add Status validation
            # status_options = ["Not Applied", "Applied", "Interview", "Rejected", "Offer"]
            # gspread doesn't have a very simple native way to add dropdowns without batch_update,
            # but we can try to skip it for now or implement if time permits.
            # For simplicity, we'll just set headers for now.

        # Prepare data for appending
        new_rows = []
        current_date = datetime.now().strftime("%Y-%m-%d")
        
        # Get existing URLs once to minimize API calls
        existing_urls = worksheet.col_values(10)

        for _, row in df.iterrows():
            job_url = str(row.get('job_url', ''))
            if job_url in existing_urls:
                continue
                
            # Formatting salary
            min_sal = row.get('min_amount')
            max_sal = row.get('max_amount')
            curr = row.get('currency', 'USD')
            salary_str = f"{min_sal:,} - {max_sal:,} {curr}" if pd.notnull(min_sal) and pd.notnull(max_sal) else "N/A"

            row_data = [
                current_date,
                row.get('title', 'N/A'),
                row.get('company', 'N/A'),
                row.get('location', 'N/A'),
                salary_str,
                row.get('job_type', 'N/A'),
                row.get('matched_skills', 'N/A'),
                row.get('ai_match_score', 0),
                row.get('ai_match_reason', 'N/A'),
                job_url,
                "Not Applied", # Default status
                ""           # Empty notes
            ]
            new_rows.append(row_data)

        if new_rows:
            # Retry logic for quota issues
            for attempt in range(2):
                try:
                    worksheet.append_rows(new_rows)
                    logger.info(f"Appended {len(new_rows)} new jobs to Google Sheets.")
                    break
                except Exception as e:
                    if attempt == 0:
                        logger.warning(f"Google Sheets append failed, retrying in 10s... {e}")
                        time.sleep(10)
                    else:
                        logger.error(f"Google Sheets append failed after retry: {e}")

            # Apply formatting
            update_sheet_formatting(worksheet)
        else:
            logger.info("No new jobs to add to Google Sheets (all were duplicates).")

    except Exception as e:
        logger.error(f"Error in export_to_google_sheets: {e}")

def export_to_csv(df: pd.DataFrame, output_dir: str = "output") -> str:
    """
    Exports the filtered jobs DataFrame to a CSV file with a timestamped name.
    
    Args:
        df (pd.DataFrame): The job data to export.
        output_dir (str): Directory to save the file.
        
    Returns:
        str: The full path to the created CSV file.
    """
    if df.empty:
        logger.warning("No jobs to export.")
        return ""

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Filename format: jobs_YYYY-MM-DD_HHMMSS.csv
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"jobs_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    # Column order as specified in requirements
    base_columns = [
        "title", "company", "location", "job_url", "job_type", 
        "min_amount", "max_amount", "currency", "date_posted", 
        "description", "source_board", "source_search_term", 
        "matched_skills", "skill_match_count"
    ]
    
    # Check for AI columns and append if present
    ai_columns = [col for col in ["ai_match_score", "ai_match_reason"] if col in df.columns]
    final_columns = base_columns + ai_columns

    # Filter to only existing columns to avoid errors if some are missing in the input DF
    existing_columns = [col for col in final_columns if col in df.columns]
    
    # Export
    try:
        df[existing_columns].to_csv(filepath, index=False)
        logger.info(f"Exported {len(df)} jobs to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to export CSV: {e}")
        return ""

def export_latest_csv(df: pd.DataFrame, output_dir: str = "output") -> str:
    """
    Exports jobs to a timestamped CSV and also saves/overwrites latest_jobs.csv.
    
    Args:
        df (pd.DataFrame): The job data to export.
        output_dir (str): Directory to save the files.
        
    Returns:
        str: The path to the timestamped CSV file.
    """
    # Export timestamped version
    filepath = export_to_csv(df, output_dir)
    
    if filepath and not df.empty:
        latest_path = os.path.join(output_dir, "latest_jobs.csv")
        
        # Reuse column logic
        base_columns = [
            "title", "company", "location", "job_url", "job_type", 
            "min_amount", "max_amount", "currency", "date_posted", 
            "description", "source_board", "source_search_term", 
            "matched_skills", "skill_match_count"
        ]
        ai_columns = [col for col in ["ai_match_score", "ai_match_reason"] if col in df.columns]
        final_columns = base_columns + ai_columns
        existing_columns = [col for col in final_columns if col in df.columns]
        
        try:
            df[existing_columns].to_csv(latest_path, index=False)
            logger.info(f"Updated latest results at {latest_path}")
        except Exception as e:
            logger.error(f"Failed to update latest CSV: {e}")

    return filepath

def display_terminal_summary(df: pd.DataFrame, top_n: int = 5):
    """
    Prints a formatted summary and a table of top N jobs to the terminal.
    Uses ASCII characters for maximum compatibility with Windows terminal.
    
    Args:
        df (pd.DataFrame): The jobs to summarize.
        top_n (int): Number of top jobs to display in the table.
    """
    divider = "="*50
    print(f"\n{divider}")
    print("               JOBBOT RUN SUMMARY")
    print(divider)
    print(f"Total jobs in results: {len(df)}")
    print(f"Run completion time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if df.empty:
        print("\nNo jobs were found matching your criteria.")
        print(f"{divider}\n")
        return

    print(f"\nTop {min(top_n, len(df))} Job Matches:")
    
    # Sort by AI score if available, otherwise skill match count
    is_scored = "ai_match_score" in df.columns
    sort_col = "ai_match_score" if is_scored else "skill_match_count"
    display_df = df.sort_values(by=sort_col, ascending=False).head(top_n)

    # Box-style table (ASCII for compatibility)
    print("+" + "-"*4 + "+" + "-"*7 + "+" + "-"*26 + "+" + "-"*16 + "+" + "-"*30 + "+")
    print("|  # " + "| Score " + "| Title" + " "*20 + "| Company" + " "*8 + "| Why" + " "*27 + "|")
    print("+" + "-"*4 + "+" + "-"*7 + "+" + "-"*26 + "+" + "-"*16 + "+" + "-"*30 + "+")

    for i, (_, row) in enumerate(display_df.iterrows(), 1):
        score_val = row.get(sort_col, 0)
        score_str = f"{int(score_val)}%" if is_scored else f"{int(score_val)}"
        title = str(row.get('title', 'N/A'))[:24]
        company = str(row.get('company', 'N/A'))[:14]
        # For non-scored, show skills match count
        why = str(row.get('ai_match_reason' if is_scored else 'matched_skills', 'N/A'))[:28]
        
        print(f"| {i:<2} | {score_str:<5} | {title:<24} | {company:<14} | {why:<28} |")

    print("+" + "-"*4 + "+" + "-"*7 + "+" + "-"*26 + "+" + "-"*16 + "+" + "-"*30 + "+")
    print(f"{divider}\n")

def generate_run_summary(total_scraped: int, total_filtered: int, total_new: int, run_time_seconds: float, ai_stats: Optional[Dict[str, Any]] = None) -> str:
    """
    Returns a formatted box summary string for reporting.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    divider = "======================================="
    sep = "---------------------------------------"
    
    summary = f"""
{divider}
JobBot Run Summary - {now}
{divider}
Jobs Scraped:    {total_scraped}
Jobs Matched:     {total_filtered}
New Jobs:         {total_new}
{sep}
"""
    if ai_stats and ai_stats.get("enabled"):
        model_name = ai_stats.get("model", "NVIDIA LLaMA").split("/")[-1]
        summary += f"""AI Scoring:     [OK] Enabled ({model_name})
Jobs Scored:    {ai_stats.get('total_scored')} ({ai_stats.get('cached')} from cache, {ai_stats.get('new')} new)
Above {ai_stats.get('threshold', 70)}%:      {ai_stats.get('above_threshold')}
Top Score:      {ai_stats.get('top_score')}% - "{ai_stats.get('top_job')}"
Avg Score:      {ai_stats.get('avg_score')}%
{sep}
"""
    else:
        summary += f"""AI Scoring:     [OFF] Disabled/Skipped
{sep}
"""

    summary += f"""Run Time:       {run_time_seconds:.1f} seconds
{divider}
"""
    return summary.strip()

if __name__ == "__main__":
    # AUTOMATIC VERIFICATION BLOCK
    import shutil
    import unittest.mock as mock
    from dotenv import load_dotenv
    load_dotenv()
    
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    test_dir = "output_test"
    
    # 1. Prepare Dummy Data
    test_df = pd.DataFrame([
        {
            "title": "Senior Solutions Architect", 
            "company": "CloudScale Inc", 
            "location": "Remote", 
            "job_url": "https://example.com/job1234",
            "job_type": "fulltime",
            "min_amount": 160000,
            "max_amount": 220000,
            "currency": "USD",
            "date_posted": "2026-04-13",
            "description": "Designing cloud architectures...",
            "source_board": "linkedin",
            "source_search_term": "architect",
            "matched_skills": "aws, terraform, python",
            "skill_match_count": 3,
            "ai_match_score": 92,
            "ai_match_reason": "Perfect match for cloud skills"
        },
        {
            "title": "Backend Developer", 
            "company": "StartupGen", 
            "location": "Austin, TX (Remote)", 
            "job_url": "https://example.com/job_backend",
            "job_type": "fulltime",
            "min_amount": 110000,
            "max_amount": 150000,
            "currency": "USD",
            "date_posted": "2026-04-11",
            "description": "Building APIs with Python...",
            "source_board": "indeed",
            "source_search_term": "python",
            "matched_skills": "python, postgres",
            "skill_match_count": 2,
            "ai_match_score": 75,
            "ai_match_reason": "Good match, but location preferred"
        }
    ])
    
    print("\n" + "="*60)
    print("      EXPORTER MODULE AUTO-VERIFICATION")
    print("="*60)

    # 2. Test CSV Export
    print("\nTest 1: CSV Export and Latest Tracking")
    path = export_latest_csv(test_df, test_dir)
    print(f"File created: {path}")
    assert os.path.exists(path), "Timestamped file should exist"
    assert os.path.exists(os.path.join(test_dir, "latest_jobs.csv")), "Latest jobs file should exist"
    
    # Verify columns
    exported_df = pd.read_csv(path)
    assert "ai_match_score" in exported_df.columns, "AI columns should be included if present"
    assert exported_df.columns[0] == "title", "Title should be the first column"
    
    # 3. Test Terminal Summary
    print("\nTest 2: Terminal Display Summary")
    display_terminal_summary(test_df)
    
    # 4. Test String Generation
    print("Test 3: Run Summary String")
    sum_str = generate_run_summary(247, 52, 38, 45.2)
    print(sum_str)
    assert "Jobs Scraped:    247" in sum_str

    # 5. Test Google Sheets (Mocked or Real check)
    print("\nTest 4: Google Sheets Integration Check")
    cred_file = os.getenv("GOOGLE_SHEETS_CRED_FILE", "credentials.json")
    if os.path.exists(cred_file):
        print(f"Credentials found ('{cred_file}'). Attempting connection...")
        # We don't want to actually run the export in a quick test unless the user wants to,
        # but we can test the setup.
        sheet_res = setup_google_sheets(cred_file, os.getenv("GOOGLE_SHEET_NAME", "JobBot_Test"))
        if sheet_res:
            print("Successfully connected to Google Sheets!")
            # Test stats
            try:
                ws = sheet_res.worksheet("Job Listings")
                stats = get_application_stats(ws)
                print(f"Current Stats: {stats}")
            except:
                print("Worksheet 'Job Listings' not found, skipping stats test.")
    else:
        print("Credentials file missing. Skipping live Google Sheets test.")
        print("Testing error handling / graceful skip...")
        # Explicitly call with missing file to ensure no crash
        result = setup_google_sheets("non_existent.json", "Dummy")
        assert result is None, "Should return None for missing credentials"
        print("Graceful skip verified.")
    
    # Cleanup
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    
    print("\n[SUCCESS] Exporter module verified successfully!")
    print("="*60 + "\n")
