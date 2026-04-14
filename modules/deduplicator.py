import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from modules.utils import retry

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

@retry(max_attempts=3, delay=1)
def update_history(jobs: pd.DataFrame, history_file: str = "output/job_history.csv"):
    """Appends new jobs to the history CSV with retry logic."""
    if jobs.empty: return
    os.makedirs(os.path.dirname(history_file), exist_ok=True)

    if 'first_seen_date' not in jobs.columns:
        jobs = jobs.copy()
        jobs['first_seen_date'] = datetime.now().strftime("%Y-%m-%d")

    file_exists = os.path.isfile(history_file)
    try:
        jobs.to_csv(history_file, mode='a', index=False, header=not file_exists)
        logger.debug(f"Added {len(jobs)} jobs to history: {history_file}")
    except Exception as e:
        logger.error(f"IO Error updating history: {e}")
        raise

def deduplicate_with_history(new_jobs: pd.DataFrame, history_file: str = "output/job_history.csv") -> pd.DataFrame:
    """Filters out already seen jobs based on URL or Title|Company."""
    if new_jobs.empty: return new_jobs
    if not os.path.exists(history_file):
        update_history(new_jobs, history_file)
        return new_jobs

    try:
        history_df = pd.read_csv(history_file)
        if history_df.empty:
            update_history(new_jobs, history_file)
            return new_jobs
    except Exception as e:
        logger.warning(f"History file unreadable ({e}). Treating all as new.")
        update_history(new_jobs, history_file)
        return new_jobs

    def norm(s): return s.fillna('').astype(str).str.lower().str.strip()
    
    hist_urls = set(history_df['job_url'].dropna().unique()) if 'job_url' in history_df.columns else set()
    hist_keys = set((norm(history_df['title']) + "|" + norm(history_df['company'])).unique())

    def is_new(row):
        url = str(row.get('job_url', ''))
        if url and url in hist_urls: return False
        key = f"{str(row.get('title', '')).lower().strip()}|{str(row.get('company', '')).lower().strip()}"
        return key not in hist_keys

    truly_new = new_jobs[new_jobs.apply(is_new, axis=1)].copy()
    diff = len(new_jobs) - len(truly_new)
    if diff > 0:
        logger.info(f"Deduplicator: Found {diff} duplicates. {len(truly_new)} new unique jobs.")

    if not truly_new.empty:
        update_history(truly_new, history_file)

    return truly_new

def get_history_stats(history_file: str = "output/job_history.csv") -> dict:
    """Calculates statistics from historical data."""
    stats = {"total_seen": 0, "today": 0, "week": 0, "top_companies": {}}
    if not os.path.exists(history_file): return stats

    try:
        df = pd.read_csv(history_file)
        if df.empty: return stats
        stats["total_seen"] = len(df)
        
        if 'first_seen_date' in df.columns:
            df['first_seen_date'] = pd.to_datetime(df['first_seen_date'], errors='coerce')
            today = datetime.now().date()
            stats["today"] = len(df[df['first_seen_date'].dt.date == today])
            stats["week"] = len(df[df['first_seen_date'].dt.date >= (today - timedelta(days=7))])
        
        if 'company' in df.columns:
            stats["top_companies"] = df['company'].value_counts().head(5).to_dict()
    except Exception as e:
        logger.error(f"Error reading stats: {e}")
    
    return stats

if __name__ == "__main__":
    from modules.logger_setup import setup_logging
    setup_logging()
    logger.info("Deduplicator test session.")
