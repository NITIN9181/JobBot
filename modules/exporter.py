import pandas as pd
import os
import logging
from datetime import datetime
from typing import Optional

# Set up logging for this module
logger = logging.getLogger(__name__)

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
    table_divider = "-" * 110
    print(table_divider)
    
    # Simple table header
    header = f"{'Title':<30} | {'Company':<20} | {'Salary Range':<20} | {'Score':<6} | {'URL'}"
    print(header)
    print(table_divider)

    # Sort by AI score if available, otherwise skill match count
    sort_col = "ai_match_score" if "ai_match_score" in df.columns else "skill_match_count"
    display_df = df.sort_values(by=sort_col, ascending=False).head(top_n)

    for _, row in display_df.iterrows():
        title = str(row.get('title', 'N/A'))[:28]
        company = str(row.get('company', 'N/A'))[:18]
        
        # Format salary range
        min_sal = row.get('min_amount')
        max_sal = row.get('max_amount')
        if pd.notnull(min_sal) and pd.notnull(max_sal):
            salary = f"${int(min_sal/1000)}K - ${int(max_sal/1000)}K"
        elif pd.notnull(min_sal):
            salary = f"${int(min_sal/1000)}K+"
        elif pd.notnull(max_sal):
            salary = f"Up to ${int(max_sal/1000)}K"
        else:
            salary = "Not listed"
            
        score = row.get(sort_col, "N/A")
        url = str(row.get('job_url', ''))[:50]
        
        print(f"{title:<30} | {company:<20} | {salary:<20} | {score:<6} | {url}")

    print(table_divider)
    print(f"{divider}\n")

def generate_run_summary(total_scraped: int, total_filtered: int, total_new: int, run_time_seconds: float) -> str:
    """
    Returns a formatted box summary string for reporting.
    Uses ASCII characters for maximum compatibility.
    
    Args:
        total_scraped (int): Jobs found before filtering.
        total_filtered (int): Jobs remaining after filtering.
        total_new (int): Jobs not already in history.
        run_time_seconds (float): Execution time.
        
    Returns:
        str: Multi-line summary string.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    divider = "="*40
    
    summary = f"""
{divider}
JobBot Run Summary - {now}
{divider}
Jobs Scraped:    {total_scraped}
Jobs Matched:     {total_filtered}
New Jobs:         {total_new}
Run Time:       {run_time_seconds:.1f} seconds
{divider}
"""
    return summary.strip()

if __name__ == "__main__":
    # AUTOMATIC VERIFICATION BLOCK
    import shutil
    
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    test_dir = "output_test"
    
    # 1. Prepare Dummy Data
    test_df = pd.DataFrame([
        {
            "title": "Senior Solutions Architect", 
            "company": "CloudScale Inc", 
            "location": "Remote", 
            "job_url": "https://example.com/job123456789012345678901234567890",
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
            "ai_match_reason": "Good match, but location prefered"
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
    
    # Cleanup
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    
    print("\n[SUCCESS] Exporter module verified successfully!")
    print("="*60 + "\n")
