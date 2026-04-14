import argparse
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Optional

from config import get_config
from modules.scraper import scrape_all_jobs
from modules.filter_engine import filter_jobs, remove_duplicates
from modules.deduplicator import deduplicate_with_history
from modules.exporter import (
    export_to_csv, export_latest_csv, 
    display_terminal_summary, generate_run_summary,
    export_to_google_sheets
)
from modules.scheduler import log_run, run_once_now, start_scheduler
from modules.notifier import send_notifications, send_email_digest, send_telegram_message
from modules.scorer import score_all_jobs, score_jobs_batch

# Set up logging
logger = logging.getLogger("JobBot.Main")

def print_banner():
    """Prints the startup banner."""
    banner = """
+-----------------------------------+
|            JobBot v1.0            |
|   Remote Job Search Automation    |
+-----------------------------------+
    """
    print(banner)

def run_job_search(test_mode: bool = False):
    """
    Orchestrates the entire job search pipeline.
    
    Args:
        test_mode: If True, limit the number of results per site to 5.
    """
    start_time = datetime.now()
    try:
        # Step 1 — Load Config
        config = get_config()
        if test_mode:
            config["results_per_site"] = 5
            logger.info("Running in TEST MODE (results_per_site=5)")
        
        logger.info("Configuration loaded successfully")

        # Step 2 — Scrape Jobs
        raw_jobs = scrape_all_jobs(config)
        logger.info(f"Scraped {len(raw_jobs)} raw jobs")
        
        if raw_jobs.empty:
            logger.warning("No jobs found. Exiting gracefully.")
            log_run("success", 0, 0, "No jobs found during scrape")
            return

        # Step 3 — Filter Jobs
        filtered_jobs = filter_jobs(raw_jobs, config)
        filtered_jobs = remove_duplicates(filtered_jobs)
        logger.info(f"Filtered down to {len(filtered_jobs)} matching jobs")

        # Step 4 — Deduplicate Against History
        new_jobs = deduplicate_with_history(filtered_jobs)
        logger.info(f"{len(new_jobs)} new jobs (not seen before)")

        # Step 4.1 — AI Scoring (Phase 3.2 Update)
        ai_stats = None
        run_ai = config.get("ai_scoring", {}).get("enabled", False)
        
        # Check CLI overrides
        if "--no-ai" in sys.argv:
            run_ai = False
            logger.info("AI scoring disabled via --no-ai flag")
        
        if run_ai and not os.getenv("NVIDIA_API_KEY"):
            logger.warning("AI scoring enabled but NVIDIA_API_KEY not found in .env. Skipping...")
            print("\n[!] NVIDIA_API_KEY missing. Get your free key at https://build.nvidia.com")
            run_ai = False

        if run_ai and not new_jobs.empty:
            logger.info("Step 4.1: Starting AI Scoring...")
            
            # Check for batch mode override or config
            use_batch = "--batch-ai" in sys.argv or config.get("ai_scoring", {}).get("batch_mode", False)
            
            from modules.scorer import score_jobs_batch, score_all_jobs
            if use_batch:
                new_jobs, ai_stats = score_jobs_batch(new_jobs, config)
            else:
                new_jobs, ai_stats = score_all_jobs(new_jobs, config)
            
            if ai_stats:
                logger.info(f"AI scored {ai_stats['total_scored']} jobs. Top score: {ai_stats['top_score']}%")
                logger.info(f"Model used: {ai_stats['model']} via NVIDIA API")
                ai_stats["threshold"] = config.get("ai_scoring", {}).get("min_score", 70)
        elif not new_jobs.empty:
            logger.info("AI scoring skipped.")

        # Step 5 — Export Results
        if not new_jobs.empty:
            export_to_csv(new_jobs)
            export_latest_csv(new_jobs)
            export_to_google_sheets(new_jobs, config)
            display_terminal_summary(new_jobs)
        else:
            logger.info("No new jobs to export.")

        # Step 6 — Send Notifications
        notif_summary = ""
        if not new_jobs.empty:
            try:
                logger.info("Sending notifications...")
                notif_results = send_notifications(new_jobs, config)
                
                # Format notification summary string
                email_status = "✅ Email sent" if notif_results.get("email_sent") else ("⚠️ Email disabled" if not notif_results.get("email_enabled") else "❌ Email failed")
                tg_status = "✅ Telegram sent" if notif_results.get("telegram_sent") else ("⚠️ Telegram disabled" if not notif_results.get("telegram_enabled") else "❌ Telegram failed")
                notif_summary = f"Notifications:  {email_status} | {tg_status}"
                
                logger.info("Notifications processed")
                print(f"\n{notif_summary}")
            except Exception as ne:
                logger.error(f"Notification error: {ne}")
                notif_summary = "Notifications:  ❌ Failed (Error)"
        else:
            logger.info("No new jobs to notify about")
            notif_summary = "Notifications:  Skipped (No new jobs)"

        # Step 7 — Log Run
        elapsed = (datetime.now() - start_time).total_seconds()
        log_run("success", len(raw_jobs), len(new_jobs))
        
        # Display final box summary if not empty
        if not new_jobs.empty:
            summary_box = generate_run_summary(len(raw_jobs), len(filtered_jobs), len(new_jobs), elapsed, ai_stats)
            print(f"\n{summary_box}")
            print(notif_summary)
        
    except Exception as e:
        error_msg = f"Error during job search: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        log_run("failure", 0, 0, error_msg)
        if not test_mode and "--schedule" not in sys.argv:
             # In manual runs, we might want to see the error clearly
             print(f"\n[CRITICAL ERROR] {e}")

def main():
    """Parsing arguments and initiating the requested mode."""
    parser = argparse.ArgumentParser(description="JobBot - Remote Job Search Automation")
    parser.add_argument("--now", action="store_true", help="Run the job search immediately once")
    parser.add_argument("--schedule", action="store_true", help="Start the daily scheduler")
    parser.add_argument("--test", action="store_true", help="Run with test config (only 5 results per site)")
    parser.add_argument("--test-email", action="store_true", help="Send a test email with dummy data")
    parser.add_argument("--test-telegram", action="store_true", help="Send a test Telegram message")
    parser.add_argument("--no-ai", action="store_true", help="Skip AI scoring even if enabled in config")
    parser.add_argument("--batch-ai", action="store_true", help="Use batch scoring mode (faster)")
    parser.add_argument("--rescore", action="store_true", help="Clear the score cache and re-score all jobs fresh")
    
    args = parser.parse_args()
    
    # Handle Cache Clearing
    if args.rescore:
        cache_path = "output/score_cache.json"
        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
                print("[OK] Score cache cleared. All jobs will be freshly scored.")
            except Exception as e:
                print(f"[ERROR] Failed to clear cache: {e}")
        else:
            print("[INFO] No score cache found to clear.")
    
    print_banner()
    
    if args.test:
        run_job_search(test_mode=True)
    elif args.schedule:
        # Get run time from config or default to 09:00
        config = get_config()
        run_time = config.get("scheduler_time", "09:00")
        start_scheduler(lambda: run_job_search(), run_time=run_time)
    elif args.test_email:
        config = get_config()
        print("Sending test email...")
        import pandas as pd
        dummy_jobs = pd.DataFrame([
            {'title': 'Test Job 1', 'company': 'Test Co', 'job_url': 'https://google.com', 'min_amount': 100000, 'max_amount': 150000, 'currency': 'USD', 'matched_skills': ['python'], 'ai_match_score': 95, 'ai_match_reason': 'Strong Python match'},
            {'title': 'Test Job 2', 'company': 'Test Inc', 'job_url': 'https://google.com', 'min_amount': 80000, 'max_amount': 100000, 'currency': 'USD', 'matched_skills': ['java'], 'ai_match_score': 75, 'ai_match_reason': 'Good match but missing some skills'},
            {'title': 'Test Job 3', 'company': 'Test Ltd', 'job_url': 'https://google.com', 'min_amount': None, 'max_amount': None, 'currency': 'USD', 'matched_skills': ['c++'], 'ai_match_score': 55, 'ai_match_reason': 'Low match'}
        ])
        success = send_email_digest(dummy_jobs, config)
        print(f"Test email {'sent successfully!' if success else 'failed. Check logs.'}")
        
    elif args.test_telegram:
        config = get_config()
        print("Sending test Telegram message...")
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            print("❌ Error: Telegram credentials missing in .env")
        else:
            success = send_telegram_message("🤖 JobBot Telegram test — connection successful!", token, chat_id)
            print(f"Test message {'sent successfully!' if success else 'failed. Check logs.'}")
            
    elif args.now:
        run_once_now(lambda: run_job_search())
    else:
        # Default behavior: run once immediately
        run_once_now(lambda: run_job_search())

if __name__ == "__main__":
    main()
