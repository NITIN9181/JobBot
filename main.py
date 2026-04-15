# Usage:
# python main.py --now              # Full pipeline (all sources + verification)
# python main.py --now --no-extended # Standard boards only
# python main.py --now --no-verify   # Skip AI verification
# python main.py --now --no-ai       # Skip AI scoring
# python main.py --extended-only     # Only extended sources
# python main.py --sources           # Show all available sources
# python main.py --test              # Quick test with limited results

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

# --- Optional module imports (fail gracefully if deps missing) ---
try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False

try:
    from modules.web_scraper import scrape_all_sources
    _WEB_SCRAPER_AVAILABLE = True
except ImportError as _e:
    _WEB_SCRAPER_AVAILABLE = False
    _WEB_SCRAPER_IMPORT_ERROR = str(_e)

try:
    from modules.india_filter import apply_india_fresher_filters
    _INDIA_FILTER_AVAILABLE = True
except ImportError as _e:
    _INDIA_FILTER_AVAILABLE = False
    _INDIA_FILTER_IMPORT_ERROR = str(_e)

try:
    from modules.verifier import verify_all_jobs, get_verification_summary
    _VERIFIER_AVAILABLE = True
except ImportError as _e:
    _VERIFIER_AVAILABLE = False
    _VERIFIER_IMPORT_ERROR = str(_e)

# Initialize centralized logging
logger = setup_logging()

# Log any failed optional imports
if not _WEB_SCRAPER_AVAILABLE:
    logger.warning(f"web_scraper module unavailable: {_WEB_SCRAPER_IMPORT_ERROR}. Extended sources disabled.")
if not _INDIA_FILTER_AVAILABLE:
    logger.warning(f"india_filter module unavailable: {_INDIA_FILTER_IMPORT_ERROR}. India/fresher filters disabled.")
if not _VERIFIER_AVAILABLE:
    logger.warning(f"verifier module unavailable: {_VERIFIER_IMPORT_ERROR}. AI verification disabled.")


def signal_handler(sig, frame):
    """Handles SIGINT and SIGTERM for graceful shutdown."""
    print("\n" + "!"*40)
    logger.info("Interrupt received. Shutting down gracefully...")
    print("Shutting down gracefully...")
    print("!"*40)
    sys.exit(0)

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def print_banner():
    """Prints the startup banner."""
    banner = r"""
      ██╗ ██████╗ ██████╗ ██████╗  ██████╗ ████████╗
      ██║██╔═══██╗██╔══██╗██╔══██╗██╔═══██╗╚══██╔══╝
  ██╗ ██║██║   ██║██████╔╝██████╔╝██║   ██║   ██║   
  ██║ ██║██║   ██║██╔══██╗██╔══██╗██║   ██║   ██║   
  ╚█████╔╝╚██████╔╝██████╔╝██████╔╝╚██████╔╝   ██║   
   ╚════╝  ╚═════╝ ╚═════╝ ╚═════╝  ╚═════╝    ╚═╝   
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
            url = "https://integrate.api.nvidia.com/v1/models"
            headers = {"Authorization": f"Bearer {api_key}"}
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                statuses["AI API (NVIDIA)"] = "[OK] Authenticated"
            else:
                statuses["AI API (NVIDIA)"] = f"[!!] Error ({res.status_code})"
        except Exception as e:
            statuses["AI API (NVIDIA)"] = "[!!] Reachability Error"
            logger.error(f"Health check AI API failure: {e}")
    else:
        statuses["AI API (NVIDIA)"] = "[--] Not configured"

    # 2. Gmail / SMTP
    if config.get("notifications", {}).get("email_enabled"):
        email = os.getenv("GMAIL_ADDRESS")
        password = os.getenv("GMAIL_APP_PASSWORD")
        if email and password:
            try:
                server = smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=10)
                server.login(email, password)
                server.quit()
                statuses["Email (Gmail)"] = "[OK] Connected"
            except Exception as e:
                statuses["Email (Gmail)"] = "[!!] Auth/Connection Failure"
                logger.error(f"Health check Email failure: {e}")
        else:
            statuses["Email (Gmail)"] = "[--] Credentials missing"
    else:
        statuses["Email (Gmail)"] = "[--] Disabled in config"

    # 3. Telegram
    if config.get("notifications", {}).get("telegram_enabled"):
        tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        if tg_token:
            try:
                url = f"https://api.telegram.org/bot{tg_token}/getMe"
                res = requests.get(url, timeout=10)
                if res.status_code == 200:
                    statuses["Telegram Bot"] = "[OK] Reachable"
                else:
                    statuses["Telegram Bot"] = f"[!!] Error ({res.status_code})"
            except Exception as e:
                statuses["Telegram Bot"] = "[!!] Reachability Error"
                logger.error(f"Health check Telegram failure: {e}")
        else:
            statuses["Telegram Bot"] = "[--] Token missing"
    else:
        statuses["Telegram Bot"] = "[--] Disabled in config"

    # 4. Google Sheets
    sheet_name = os.getenv("GOOGLE_SHEET_NAME")
    if sheet_name:
        try:
            spreadsheet, _ = setup_google_sheets(sheet_name)
            if spreadsheet:
                statuses["Google Sheets"] = "[OK] Connected"
            else:
                statuses["Google Sheets"] = "[!!] Connection Failed"
        except Exception as e:
            statuses["Google Sheets"] = "[!!] Auth Error"
            logger.error(f"Health check Google Sheets failure: {e}")
    else:
        statuses["Google Sheets"] = "[--] Not configured"

    # 5. Job Boards (Basic connectivity test)
    try:
        res = requests.get("https://www.indeed.com", timeout=10)
        statuses["Indeed"] = "[OK] Reachable" if res.status_code < 400 else f"[!!] Blocked ({res.status_code})"
    except Exception:
        statuses["Indeed"] = "[!!] Unreachable"

    try:
        res = requests.get("https://www.linkedin.com/jobs", timeout=10)
        statuses["LinkedIn"] = "[OK] Reachable" if res.status_code < 400 else f"[!!] Blocked ({res.status_code})"
    except Exception:
        statuses["LinkedIn"] = "[!!] Unreachable"

    # Print Dashboard
    print("\nService Status:")
    for service, status in statuses.items():
        print(f"  {service + ':':<18} {status}")
    print("\n" + "="*40 + "\n")


def _build_source_breakdown(raw_jobs, extended_jobs_count: int = 0) -> str:
    """
    Builds a human-readable source breakdown string from the raw_jobs DataFrame.
    E.g.: Sources: JobSpy(45) + RemoteOK(23) + Himalayas(12) = 80 total
    """
    import pandas as pd
    if raw_jobs.empty:
        return "Sources: None"

    parts = []
    total = len(raw_jobs)

    if 'source_platform' in raw_jobs.columns:
        counts = raw_jobs['source_platform'].value_counts()
        for platform, count in counts.items():
            parts.append(f"{platform}({count})")
    else:
        parts.append(f"JobSpy({total})")

    source_str = " + ".join(parts)
    return f"Sources: {source_str} = {total} total"


def run_job_search(test_mode: bool = False, extended_only: bool = False):
    """
    Orchestrates the entire job search pipeline.

    Pipeline:
      Step 1   - Load Config
      Step 2   - Scrape standard boards (scraper.py)
      Step 2.1 - Scrape extended sources (web_scraper.py)
      Step 2.2 - Merge all results
      Step 3   - India + Fresher filters (india_filter.py)
      Step 4   - Standard filters (filter_engine.py) + dedup
      Step 5   - Deduplicate against history
      Step 6   - AI Verification (verifier.py)
      Step 7   - AI Scoring (scorer.py)
      Step 8   - Export + Notify
    """
    import pandas as pd

    start_time = datetime.now()
    try:
        # ------------------------------------------------------------------ #
        # Step 1 — Load Config
        # ------------------------------------------------------------------ #
        config = get_config()
        if test_mode:
            config["results_per_site"] = 5
            logger.info("Running in TEST MODE (results_per_site=5)")

        logger.info("Starting pipeline execution")

        # ------------------------------------------------------------------ #
        # Step 2 — Scrape Standard Boards
        # ------------------------------------------------------------------ #
        if extended_only:
            raw_jobs = pd.DataFrame()
            logger.info("Running in EXTENDED-ONLY mode — skipping standard job boards")
        else:
            raw_jobs = scrape_all_jobs(config)
            logger.info(f"Scraped {len(raw_jobs)} raw jobs from standard boards")
            # Tag source for breakdown display
            if not raw_jobs.empty and 'source_platform' not in raw_jobs.columns:
                raw_jobs['source_platform'] = 'JobSpy'

        # ------------------------------------------------------------------ #
        # Step 2.1 — Scrape Extended Sources
        # ------------------------------------------------------------------ #
        extended_jobs = pd.DataFrame()
        extended_sources_enabled = config.get("extended_sources", {}).get("enabled", False)

        if not _WEB_SCRAPER_AVAILABLE:
            logger.warning("web_scraper module not available — skipping extended sources")
        elif extended_sources_enabled and "--no-extended" not in sys.argv:
            try:
                logger.info("Step 2.1: Scraping extended sources (RemoteOK, Himalayas, Jobicy, WWR)")
                extended_jobs = scrape_all_sources(config)
                logger.info(f"Extended sources returned {len(extended_jobs)} jobs")
            except Exception as ext_err:
                logger.error(f"Extended scraping failed: {ext_err}")
                logger.debug(traceback.format_exc())
                extended_jobs = pd.DataFrame()
        else:
            logger.info("Extended sources: Disabled or skipped (--no-extended)")

        # ------------------------------------------------------------------ #
        # Step 2.2 — Merge All Sources
        # ------------------------------------------------------------------ #
        if not extended_jobs.empty:
            raw_jobs = pd.concat([raw_jobs, extended_jobs], ignore_index=True)
            logger.info(f"Merged total: {len(raw_jobs)} jobs from all sources")

        # Build source breakdown string early (before any filtering)
        source_breakdown = _build_source_breakdown(raw_jobs)

        if raw_jobs.empty:
            logger.warning("No jobs found during scraping (all sources)")
            log_run("success", 0, 0, "No jobs found during scrape")
            return

        # ------------------------------------------------------------------ #
        # Step 3 — India + Fresher Filters
        # ------------------------------------------------------------------ #
        filtered_jobs = raw_jobs.copy()

        if not _INDIA_FILTER_AVAILABLE:
            logger.warning("india_filter module not available — skipping India/fresher filters")
        elif config.get("target_country") and config.get("target_country") != "any":
            try:
                logger.info("Step 3: Applying India eligibility + fresher filters")
                filtered_jobs = apply_india_fresher_filters(raw_jobs, config)
                logger.info(f"After India+Fresher filter: {len(filtered_jobs)} jobs remaining")
            except Exception as if_err:
                logger.error(f"India/Fresher filter failed: {if_err}")
                logger.debug(traceback.format_exc())
                filtered_jobs = raw_jobs.copy()
        else:
            logger.info("Step 3: India/Fresher filter skipped (target_country=any or not set)")

        # ------------------------------------------------------------------ #
        # Step 4 — Generic Criteria Filtering + In-batch dedup
        # ------------------------------------------------------------------ #
        try:
            filtered_jobs = filter_jobs(filtered_jobs, config)
            filtered_jobs = remove_duplicates(filtered_jobs)
            logger.info(f"Filtered down to {len(filtered_jobs)} matching jobs")
        except Exception as fe:
            logger.error(f"Filter engine error: {fe}")
            logger.debug(traceback.format_exc())

        # ------------------------------------------------------------------ #
        # Step 5 — Deduplicate Against History
        # ------------------------------------------------------------------ #
        new_jobs = deduplicate_with_history(filtered_jobs)
        logger.info(f"{len(new_jobs)} new jobs (not seen before)")

        # ------------------------------------------------------------------ #
        # Step 6 — AI Verification
        # ------------------------------------------------------------------ #
        verification_stats = None
        run_verification = config.get("verification", {}).get("enabled", False)

        if "--no-verify" in sys.argv:
            run_verification = False
            logger.info("AI verification disabled via override flag")

        if run_verification and not os.getenv("NVIDIA_API_KEY"):
            logger.warning("Verification enabled but NVIDIA_API_KEY missing. Skipping...")
            run_verification = False

        if not _VERIFIER_AVAILABLE:
            if run_verification:
                logger.warning("verifier module not available — skipping AI verification")
            run_verification = False

        if run_verification and not new_jobs.empty:
            try:
                logger.info("Step 6: Starting AI Verification sequence")
                new_jobs, verification_stats = verify_all_jobs(new_jobs, config)
                verified_count = (
                    len(new_jobs[new_jobs['verified'] == True])
                    if 'verified' in new_jobs.columns
                    else len(new_jobs)
                )
                logger.info(f"Verification complete. {verified_count} jobs passed verification.")
                print(f"\n{get_verification_summary(verification_stats)}")
            except Exception as ve:
                logger.error(f"AI Verification step failed: {ve}")
                logger.debug(traceback.format_exc())
                verification_stats = None

        # ------------------------------------------------------------------ #
        # Step 7 — AI Scoring
        # ------------------------------------------------------------------ #
        ai_stats = None
        run_ai = config.get("ai_scoring", {}).get("enabled", False)

        if "--no-ai" in sys.argv:
            run_ai = False
            logger.info("AI scoring disabled via override flag")

        if run_ai and not os.getenv("NVIDIA_API_KEY"):
            logger.warning("AI scoring enabled but NVIDIA_API_KEY missing. Skipping...")
            run_ai = False

        if run_ai and not new_jobs.empty:
            try:
                logger.info("Step 7: Starting AI Scoring sequence")
                use_batch = "--batch-ai" in sys.argv or config.get("ai_scoring", {}).get("batch_mode", False)
                if use_batch:
                    new_jobs, ai_stats = score_jobs_batch(new_jobs, config)
                else:
                    new_jobs, ai_stats = score_all_jobs(new_jobs, config)

                threshold = config.get("ai_scoring", {}).get("min_score", 70)
                top_matches = new_jobs[new_jobs["ai_match_score"] >= threshold].copy()
                logger.info(f"AI scoring complete. {len(top_matches)} matches above threshold.")
            except Exception as ae:
                logger.error(f"AI Scoring step failed: {ae}")
                logger.debug(traceback.format_exc())
                top_matches = new_jobs.copy()
        else:
            top_matches = new_jobs.copy()

        # ------------------------------------------------------------------ #
        # Step 8 — Export Results
        # ------------------------------------------------------------------ #
        gs_status = None
        if not new_jobs.empty:
            export_to_csv(new_jobs)
            export_latest_csv(new_jobs)

            logger.info("Exporting results to Google Sheets tracker")
            gs_status = export_to_google_sheets(new_jobs, config)

            display_terminal_summary(new_jobs)
        else:
            logger.info("No new jobs to process for export")

        # ------------------------------------------------------------------ #
        # Step 8.1 — Send Notifications
        # ------------------------------------------------------------------ #
        notif_summary = ""
        if not top_matches.empty:
            try:
                logger.info(f"Sending notifications for {len(top_matches)} top matches")
                notif_results = send_notifications(top_matches, config)

                email_status = (
                    "[OK] Email sent" if notif_results.get("email_sent")
                    else ("[--] Email disabled" if not notif_results.get("email_enabled")
                          else "[!!] Email failed")
                )
                tg_status = (
                    "[OK] Telegram sent" if notif_results.get("telegram_sent")
                    else ("[--] Telegram disabled" if not notif_results.get("telegram_enabled")
                          else "[!!] Telegram failed")
                )
                notif_summary = f"Notifications:  {email_status} | {tg_status}"
            except Exception as ne:
                logger.error(f"Notification error: {ne}")
                notif_summary = "Notifications:  [XX] Failed (Check Logs)"
        else:
            notif_summary = "Notifications:  Skipped (No matches)"

        # ------------------------------------------------------------------ #
        # Step 9 — Log Run & Print Summary
        # ------------------------------------------------------------------ #
        elapsed = (datetime.now() - start_time).total_seconds()
        log_run("success", len(raw_jobs), len(new_jobs))

        if not new_jobs.empty:
            summary_box = generate_run_summary(
                len(raw_jobs), len(filtered_jobs), len(new_jobs),
                elapsed, ai_stats, gs_status, verification_stats
            )
            print(f"\n{summary_box}")
            print(source_breakdown)
            print(notif_summary)

            # Verification summary (detailed block)
            if verification_stats and _VERIFIER_AVAILABLE:
                print(get_verification_summary(verification_stats))

    except Exception as e:
        error_msg = f"Critical error in pipeline: {str(e)}"
        logger.critical(error_msg)
        logger.debug(traceback.format_exc())
        log_run("failure", 0, 0, error_msg)
        print(f"\n[CRITICAL ERROR] {e}")


def main():
    """Main entry point handling arguments and execution modes."""
    parser = argparse.ArgumentParser(
        description="JobBot - Remote Job Search Automation",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # Existing flags
    parser.add_argument("--now", action="store_true", help="Run the job search immediately once")
    parser.add_argument("--schedule", action="store_true", help="Start the daily scheduler")
    parser.add_argument("--test", action="store_true", help="Run with test config (limited results)")
    parser.add_argument("--health", action="store_true", help="Run service health check")
    parser.add_argument("--test-email", action="store_true", help="Send a test notification email")
    parser.add_argument("--test-telegram", action="store_true", help="Send a test Telegram message")
    parser.add_argument("--no-ai", action="store_true", help="Skip AI scoring")
    parser.add_argument("--no-verify", action="store_true", help="Skip AI verification step")
    parser.add_argument("--batch-ai", action="store_true", help="Use batch scoring mode")
    parser.add_argument("--rescore", action="store_true", help="Clear score cache and re-score")
    parser.add_argument("--reverify", action="store_true", help="Clear verification cache")
    parser.add_argument("--stats", action="store_true", help="Show application stats from Sheets")

    # Phase 7.5 — New flags
    parser.add_argument("--no-extended", action="store_true",
                        help="Skip extended sources (RemoteOK, Himalayas, etc.)")
    parser.add_argument("--extended-only", action="store_true",
                        help="Only scrape extended sources, skip standard job boards")
    parser.add_argument("--sources", action="store_true",
                        help="Show available data sources and exit")

    args = parser.parse_args()

    # ------------------------------------------------------------------ #
    # --rescore / --reverify cache management
    # ------------------------------------------------------------------ #
    if args.rescore:
        cache_path = "output/score_cache.json"
        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
                print("[OK] Score cache cleared.")
            except Exception as e:
                print(f"[!!] Failed to clear score cache: {e}")

    if args.reverify:
        cache_path = "output/verify_cache.json"
        if os.path.exists(cache_path):
            try:
                os.remove(cache_path)
                print("[OK] Verification cache cleared.")
            except Exception as e:
                print(f"[!!] Failed to clear verification cache: {e}")

    print_banner()

    # ------------------------------------------------------------------ #
    # --sources — Show available data sources and exit
    # ------------------------------------------------------------------ #
    if args.sources:
        config = get_config()
        print("\n" + "="*40)
        print("   AVAILABLE DATA SOURCES")
        print("="*40)
        print("\nStandard Job Boards (via python-jobspy):")
        print("  Indeed:         [OK] Enabled")
        print("  LinkedIn:       [OK] Enabled")
        print("  Google Jobs:    [OK] Enabled")
        print("  ZipRecruiter:   [OK] Enabled")
        print("  Glassdoor:      [OK] Enabled")

        ext = config.get("extended_sources", {})
        status = lambda k: "[OK] Enabled" if ext.get(k, False) else "[--] Disabled"
        print("\nExtended Sources:")
        print(f"  RemoteOK:       {status('remoteok')}")
        print(f"  Himalayas:      {status('himalayas')}")
        print(f"  Jobicy:         {status('jobicy')}")
        print(f"  WeWorkRemotely: {status('weworkremotely')}")

        ver = config.get("verification", {})
        print(f"\nAI Verification:   {'[OK] Enabled' if ver.get('enabled', False) else '[--] Disabled'}")
        print(f"Target Country:    {config.get('target_country', 'Not set')}")
        print(f"Experience Level:  {config.get('experience', {}).get('level', 'Not set')}")
        print("="*40)
        return

    # ------------------------------------------------------------------ #
    # --health
    # ------------------------------------------------------------------ #
    if args.health:
        config = get_config()
        run_health_check(config)
        return

    # ------------------------------------------------------------------ #
    # --test-email / --test-telegram
    # ------------------------------------------------------------------ #
    if args.test_email:
        config = get_config()
        print("Sending test email...")
        import pandas as pd
        dummy_jobs = pd.DataFrame([{
            'title': 'Test Job 1', 'company': 'Test Co',
            'job_url': 'https://google.com',
            'min_amount': 100000, 'max_amount': 150000, 'currency': 'USD',
            'matched_skills': ['python'], 'ai_match_score': 95,
            'ai_match_reason': 'Connection test successful'
        }])
        success = send_email_digest(dummy_jobs, config)
        print(f"Result: {'[OK] Success' if success else '[!!] Failed'}")
        return

    if args.test_telegram:
        print("Sending test Telegram message...")
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            print("[!!] Error: Telegram credentials missing in .env")
        else:
            success = send_telegram_message("JobBot Telegram Connection Active!", token, chat_id)
            print(f"Result: {'[OK] Success' if success else '[!!] Failed'}")
        return

    # ------------------------------------------------------------------ #
    # --stats
    # ------------------------------------------------------------------ #
    if args.stats:
        config = get_config()
        sheet_name = os.getenv("GOOGLE_SHEET_NAME", "JobBot_Output")
        print(f"[*] Fetching stats from Google Sheet: '{sheet_name}'...")
        spreadsheet, worksheet = setup_google_sheets(sheet_name)
        if spreadsheet and worksheet:
            try:
                stats = get_application_stats(worksheet)
                display_application_stats(stats)
            except Exception as e:
                print(f"[!!] Error: Could not fetch stats. {e}")
        else:
            print("[!!] Connection error. Check credentials.")
        return

    # ------------------------------------------------------------------ #
    # --schedule
    # ------------------------------------------------------------------ #
    if args.schedule:
        config = get_config()
        run_time = config.get("scheduler_time", "09:00")
        start_scheduler(lambda: run_job_search(), run_time=run_time)
        return

    # ------------------------------------------------------------------ #
    # --test / --now / --extended-only / default
    # ------------------------------------------------------------------ #
    run_job_search(
        test_mode=args.test,
        extended_only=args.extended_only,
    )


if __name__ == "__main__":
    main()
