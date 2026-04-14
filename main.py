import argparse
import logging
import os
import sys
import traceback
import signal
import time
from datetime import datetime
from typing import Optional, Dict, Any
import io
import requests
import smtplib

# Force UTF-8 encoding for Windows terminal compatibility
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from modules.logger_setup import setup_logging
from config import get_config
from modules.scraper import scrape_all_jobs
from modules.filter_engine import filter_jobs, remove_duplicates
from modules.deduplicator import deduplicate_with_history
from modules.exporter import (
    export_to_csv, export_latest_csv, 
    display_terminal_summary, generate_run_summary,
    export_to_google_sheets, setup_google_sheets,
    get_application_stats, display_application_stats
)
from modules.scheduler import log_run, run_once_now, start_scheduler
from modules.notifier import send_notifications, send_email_digest, send_telegram_message
from modules.scorer import score_all_jobs, score_jobs_batch

# Initialize centralized logging
logger = setup_logging()

def signal_handler(sig, frame):
    """Handles SIGINT and SIGTERM for graceful shutdown."""
    print("\n" + "!"*40)
    logger.info("Interrupt received. Shutting down gracefully...")
    print("⚠️  Shutting down gracefully...")
    print("!"*40)
    
    # In a more complex app, we'd trigger cleanup here
    # Since this is a CLI pipeline, we'll mostly just ensure we don't crash uglily
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def print_banner():
    """Prints the startup banner."""
    banner = r"""
    ██╗ ██████╗ ██████╗ ██████╗  ██████╗ ████████╗
    ██║██╔═══██╗██╔══██╗██╔══██╗██╔═══██╗╚══██╔══╝
    ██║██║   ██║██████╔╝██████╔╝██║   ██║   ██║   
    ██║██║   ██║██╔══██╗██╔══██╗██║   ██║   ██║   
    ██║╚██████╔╝██████╔╝██████╔╝╚██████╔╝   ██║   
    ╚═╝ ╚═════╝ ╚═════╝ ╚═════╝  ╚═════╝    ╚═╝   
    """
    print(banner)
    print("       Remote Job Search Automation")
    print("+" + "-"*40 + "+")

def run_health_check(config: Dict[str, Any]):
    """
    Tests connectivity to all configured services and prints a status dashboard.
    """
    print("\n" + "="*40)
    print("       SERVICE HEALTH CHECK")
    print("="*40)
    
    statuses = {}
    
    # 1. Groq/NVIDIA API
    api_key = os.getenv("NVIDIA_API_KEY") or os.getenv("GROQ_API_KEY")
    if api_key:
        try:
            # Simple list models call to verify auth
            url = "https://integrate.api.nvidia.com/v1/models"
            headers = {"Authorization": f"Bearer {api_key}"}
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                statuses["AI API (NVIDIA)"] = "✅ Authenticated"
            else:
                statuses["AI API (NVIDIA)"] = f"❌ Error ({res.status_code})"
        except Exception as e:
            statuses["AI API (NVIDIA)"] = f"❌ Reachability Error"
            logger.error(f"Health check AI API failure: {e}")
    else:
        statuses["AI API (NVIDIA)"] = "⚠️ Not configured"

    # 2. Gmail / SMTP
    if config.get("notifications", {}).get("email_enabled"):
        email = os.getenv("GMAIL_ADDRESS")
        password = os.getenv("GMAIL_APP_PASSWORD")
        if email and password:
            try:
                server = smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=10)
                server.login(email, password)
                server.quit()
                statuses["Email (Gmail)"] = "✅ Connected"
            except Exception as e:
                statuses["Email (Gmail)"] = "❌ Auth/Connection Failure"
                logger.error(f"Health check Email failure: {e}")
        else:
            statuses["Email (Gmail)"] = "⚠️ Credentials missing"
    else:
        statuses["Email (Gmail)"] = "⚠️ Disabled in config"

    # 3. Telegram
    if config.get("notifications", {}).get("telegram_enabled"):
        tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if tg_token:
            try:
                url = f"https://api.telegram.org/bot{tg_token}/getMe"
                res = requests.get(url, timeout=10)
                if res.status_code == 200:
                    statuses["Telegram Bot"] = "✅ Reachable"
                else:
                    statuses["Telegram Bot"] = f"❌ Error ({res.status_code})"
            except Exception as e:
                statuses["Telegram Bot"] = "❌ Reachability Error"
                logger.error(f"Health check Telegram failure: {e}")
        else:
            statuses["Telegram Bot"] = "⚠️ Token missing"
    else:
        statuses["Telegram Bot"] = "⚠️ Disabled in config"

    # 4. Google Sheets
    sheet_name = os.getenv("GOOGLE_SHEET_NAME")
    if sheet_name:
        try:
            spreadsheet, _ = setup_google_sheets(sheet_name)
            if spreadsheet:
                statuses["Google Sheets"] = "✅ Connected"
            else:
                statuses["Google Sheets"] = "❌ Connection Failed"
        except Exception as e:
            statuses["Google Sheets"] = "❌ Auth Error"
            logger.error(f"Health check Google Sheets failure: {e}")
    else:
        statuses["Google Sheets"] = "⚠️ Not configured"

    # 5. Job Boards (Basic connectivity test)
    try:
        res = requests.get("https://www.indeed.com", timeout=10)
        statuses["Indeed"] = "✅ Reachable" if res.status_code < 400 else f"❌ Blocked ({res.status_code})"
    except:
        statuses["Indeed"] = "❌ Unreachable"

    try:
        res = requests.get("https://www.linkedin.com/jobs", timeout=10)
        statuses["LinkedIn"] = "✅ Reachable" if res.status_code < 400 else f"❌ Blocked ({res.status_code})"
    except:
        statuses["LinkedIn"] = "❌ Unreachable"

    # Print Dashboard
    print("\nService Status:")
    for service, status in statuses.items():
        print(f"├── {service + ':':<15} {status}")
    print("\n" + "="*40 + "\n")

def run_job_search(test_mode: bool = False):
    """
    Orchestrates the entire job search pipeline.
    """
    start_time = datetime.now()
    try:
        # Step 1 — Load Config
        config = get_config()
        if test_mode:
            config["results_per_site"] = 5
            logger.info("Running in TEST MODE (results_per_site=5)")
        
        logger.info("Starting pipeline execution")

        # Step 2 — Scrape Jobs
        raw_jobs = scrape_all_jobs(config)
        logger.info(f"Scraped {len(raw_jobs)} raw jobs")
        
        if raw_jobs.empty:
            logger.warning("No jobs found during scraping")
            log_run("success", 0, 0, "No jobs found during scrape")
            return

        # Step 3 — Filter Jobs
        filtered_jobs = filter_jobs(raw_jobs, config)
        filtered_jobs = remove_duplicates(filtered_jobs)
        logger.info(f"Filtered down to {len(filtered_jobs)} matching jobs")

        # Step 4 — Deduplicate Against History
        new_jobs = deduplicate_with_history(filtered_jobs)
        logger.info(f"{len(new_jobs)} new jobs (not seen before)")

        # Step 4.1 — AI Scoring
        ai_stats = None
        run_ai = config.get("ai_scoring", {}).get("enabled", False)
        
        if "--no-ai" in sys.argv:
            run_ai = False
            logger.info("AI scoring disabled via override flag")
        
        if run_ai and not os.getenv("NVIDIA_API_KEY"):
            logger.warning("AI scoring enabled but NVIDIA_API_KEY missing. Skipping...")
            run_ai = False

        if run_ai and not new_jobs.empty:
            logger.info("Step 4.1: Starting AI Scoring sequence")
            use_batch = "--batch-ai" in sys.argv or config.get("ai_scoring", {}).get("batch_mode", False)
            
            if use_batch:
                new_jobs, ai_stats = score_jobs_batch(new_jobs, config)
            else:
                new_jobs, ai_stats = score_all_jobs(new_jobs, config)
            
            threshold = config.get("ai_scoring", {}).get("min_score", 70)
            top_matches = new_jobs[new_jobs["ai_match_score"] >= threshold].copy()
            logger.info(f"AI scoring complete. {len(top_matches)} matches above threshold.")
        else:
            top_matches = new_jobs.copy()

        # Step 5 — Export Results
        gs_status = None
        if not new_jobs.empty:
            export_to_csv(new_jobs)
            export_latest_csv(new_jobs)
            
            logger.info("Exporting results to Google Sheets tracker")
            gs_status = export_to_google_sheets(new_jobs, config)
            
            display_terminal_summary(new_jobs)
        else:
            logger.info("No new jobs to process for export")

        # Step 6 — Send Notifications
        notif_summary = ""
        if not top_matches.empty:
            try:
                logger.info(f"Sending notifications for {len(top_matches)} top matches")
                notif_results = send_notifications(top_matches, config)
                
                email_status = "[OK] Email sent" if notif_results.get("email_sent") else ("[--] Email disabled" if not notif_results.get("email_enabled") else "[!!] Email failed")
                tg_status = "[OK] Telegram sent" if notif_results.get("telegram_sent") else ("[--] Telegram disabled" if not notif_results.get("telegram_enabled") else "[!!] Telegram failed")
                notif_summary = f"Notifications:  {email_status} | {tg_status}"
            except Exception as ne:
                logger.error(f"Notification error: {ne}")
                notif_summary = "Notifications:  [XX] Failed (Check Logs)"
        else:
            notif_summary = "Notifications:  Skipped (No matches)"

        # Step 7 — Log Run
        elapsed = (datetime.now() - start_time).total_seconds()
        log_run("success", len(raw_jobs), len(new_jobs))
        
        if not new_jobs.empty:
            summary_box = generate_run_summary(len(raw_jobs), len(filtered_jobs), len(new_jobs), elapsed, ai_stats, gs_status)
            print(f"\n{summary_box}")
            print(notif_summary)
        
    except Exception as e:
        error_msg = f"Critical error in pipeline: {str(e)}"
        logger.critical(error_msg)
        logger.debug(traceback.format_exc())
        log_run("failure", 0, 0, error_msg)
        print(f"\n❌ [CRITICAL ERROR] {e}")

def main():
    """Main entry point handling arguments and execution modes."""
    parser = argparse.ArgumentParser(description="JobBot - Remote Job Search Automation")
    parser.add_argument("--now", action="store_true", help="Run the job search immediately once")
    parser.add_argument("--schedule", action="store_true", help="Start the daily scheduler")
    parser.add_argument("--test", action="store_true", help="Run with test config (limited results)")
    parser.add_argument("--health", action="store_true", help="Run service health check")
    parser.add_argument("--test-email", action="store_true", help="Send a test notification email")
    parser.add_argument("--test-telegram", action="store_true", help="Send a test Telegram message")
    parser.add_argument("--no-ai", action="store_true", help="Skip AI scoring")
    parser.add_argument("--batch-ai", action="store_true", help="Use batch scoring mode")
    parser.add_argument("--rescore", action="store_true", help="Clear score cache and re-score")
    parser.add_argument("--stats", action="store_true", help="Show application stats from Sheets")
    
    args = parser.parse_args()
    
    if args.rescore:
        cache_path = "output/score_cache.json"
        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
                print("✅ Score cache cleared.")
            except Exception as e:
                print(f"❌ Failed to clear cache: {e}")
    
    print_banner()
    
    # Load config early for specific commands needing it
    if args.health:
        config = get_config()
        run_health_check(config)
        return

    if args.test:
        run_job_search(test_mode=True)
    elif args.schedule:
        config = get_config()
        run_time = config.get("scheduler_time", "09:00")
        start_scheduler(lambda: run_job_search(), run_time=run_time)
    elif args.test_email:
        config = get_config()
        print("Sending test email...")
        import pandas as pd
        dummy_jobs = pd.DataFrame([
            {'title': 'Test Job 1', 'company': 'Test Co', 'job_url': 'https://google.com', 'min_amount': 100000, 'max_amount': 150000, 'currency': 'USD', 'matched_skills': ['python'], 'ai_match_score': 95, 'ai_match_reason': 'Connection test successful'}
        ])
        success = send_email_digest(dummy_jobs, config)
        print(f"Result: {'✅ Success' if success else '❌ Failed'}")
    elif args.test_telegram:
        print("Sending test Telegram message...")
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            print("❌ Error: Telegram credentials missing in .env")
        else:
            success = send_telegram_message("JobBot Telegram Connection Active! 🤖", token, chat_id)
            print(f"Result: {'✅ Success' if success else '❌ Failed'}")
    elif args.stats:
        config = get_config()
        sheet_name = os.getenv("GOOGLE_SHEET_NAME", "JobBot_Output")
        print(f"[*] Fetching stats from Google Sheet: '{sheet_name}'...")
        spreadsheet, worksheet = setup_google_sheets(sheet_name)
        if spreadsheet and worksheet:
            try:
                stats = get_application_stats(worksheet)
                display_application_stats(stats)
            except Exception as e:
                print(f"❌ Error: Could not fetch stats. {e}")
        else:
            print("❌ Connection error. Check credentials.")
    else:
        # Default behavior: run once
        run_job_search(test_mode=args.test)

if __name__ == "__main__":
    main()
