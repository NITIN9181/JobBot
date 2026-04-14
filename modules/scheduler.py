import schedule
import time
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Callable

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

def get_next_run_time(run_time: str) -> str:
    """Calculates the time until the next scheduled run."""
    now = datetime.now()
    try:
        scheduled_time = datetime.strptime(run_time, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day
        )
    except ValueError:
        logger.error(f"Invalid run_time format: {run_time}. Expected HH:MM.")
        return "Invalid run time"

    if scheduled_time <= now:
        scheduled_time += timedelta(days=1)

    diff = scheduled_time - now
    hours, remainder = divmod(int(diff.total_seconds()), 3600)
    minutes, _ = divmod(remainder, 60)

    time_str = scheduled_time.strftime("%I:%M %p")
    return f"Next run in {hours}h {minutes}m (at {time_str})"

def log_run(status: str, jobs_found: int, jobs_matched: int, error_message: str = "", log_file: str = "logs/run_log.csv"):
    """Appends a summary of the job run to a CSV log file."""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    data = {
        "timestamp": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        "status": [status],
        "found": [jobs_found],
        "matched": [jobs_matched],
        "error": [error_message]
    }
    df = pd.DataFrame(data)
    file_exists = os.path.isfile(log_file)
    df.to_csv(log_file, mode='a', index=False, header=not file_exists)
    logger.debug(f"Run summary appended to {log_file}")

def run_once_now(job_function: Callable):
    """Executes the job function immediately."""
    logger.info("Executing immediate manual run...")
    start_time = datetime.now()
    try:
        job_function()
        status = "success"
    except Exception as e:
        logger.error(f"Manual run failed: {e}")
        status = f"failure: {str(e)}"
    
    duration = datetime.now() - start_time
    logger.info(f"Manual run finished ({duration}). Status: {status}")

def start_scheduler(job_function: Callable, run_time: str = "09:00"):
    """Starts the daily scheduler loop."""
    schedule.every().day.at(run_time).do(job_function)
    
    next_info = get_next_run_time(run_time)
    logger.info(f"Scheduler active. Running daily at {run_time}. {next_info}")
    print(f"[*] Scheduler active. {next_info}")
    
    last_heartbeat = datetime.now()
    try:
        while True:
            schedule.run_pending()
            
            # Heartbeat every 1 hour
            now = datetime.now()
            if now - last_heartbeat >= timedelta(hours=1):
                logger.info(f"Heartbeat: {get_next_run_time(run_time)}")
                last_heartbeat = now
                
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")

if __name__ == "__main__":
    from modules.logger_setup import setup_logging
    setup_logging()
    logger.info("Scheduler test session.")
