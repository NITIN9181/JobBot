import os
import smtplib
import logging
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, Any, Optional
import pandas as pd
import requests
from modules.utils import retry

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

def format_salary(min_amt: Any, max_amt: Any, currency: Optional[str] = "USD") -> str:
    """
    Formats salary range into a readable string like '$80K - $120K USD'.
    Returns 'Not listed' if values are missing.
    """
    if pd.isna(min_amt) and pd.isna(max_amt):
        return "Not listed"
    
    parts = []
    
    def to_k(val):
        if pd.isna(val):
            return None
        try:
            val_num = float(val)
            if val_num >= 1000:
                return f"${int(val_num/1000)}K"
            return f"${int(val_num)}"
        except (ValueError, TypeError):
            return str(val)

    min_str = to_k(min_amt)
    max_str = to_k(max_amt)
    
    if min_str and max_str:
        if min_str == max_str:
            res = f"{min_str}"
        else:
            res = f"{min_str} - {max_str}"
    elif min_str:
        res = f"{min_str}+"
    elif max_str:
        res = f"Up to {max_str}"
    else:
        return "Not listed"
    
    if currency:
        res += f" {currency}"
        
    return res

def send_email_digest(jobs: pd.DataFrame, config: Dict[str, Any]):
    """
    Sends an HTML email summary of the top job matches.
    """
    gmail_address = os.getenv("GMAIL_ADDRESS")
    gmail_app_password = os.getenv("GMAIL_APP_PASSWORD")
    
    if not gmail_address or not gmail_app_password:
        logger.warning("Email credentials missing (GMAIL_ADDRESS/GMAIL_APP_PASSWORD). Skipping email.")
        return False

    if jobs.empty:
        logger.info("No jobs to send in digest. Skipping email.")
        return False

    # Take top 20 jobs
    top_jobs = jobs.head(20).copy()
    count = len(top_jobs)
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    # Create the root message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"🤖 JobBot Daily Report — {date_str} | {count} New Jobs Found"
    msg['From'] = gmail_address
    msg['To'] = gmail_address

    # Build HTML Body
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; line-height: 1.6; }}
            .container {{ max-width: 800px; margin: 0 auto; padding: 20px; }}
            .header {{ background: linear-gradient(135deg, #1a73e8, #0d47a1); color: white; padding: 30px; text-align: center; border-radius: 8px 8px 0 0; }}
            .stats {{ background: #f8f9fa; padding: 15px; border-bottom: 2px solid #eee; display: flex; justify-content: space-around; }}
            .stat-item {{ text-align: center; }}
            .stat-value {{ font-size: 24px; font-weight: bold; color: #1a73e8; }}
            .stat-label {{ font-size: 14px; color: #666; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th {{ background-color: #f1f3f4; color: #5f6368; text-align: left; padding: 12px; border-bottom: 2px solid #dee2e6; }}
            td {{ padding: 12px; border-bottom: 1px solid #eee; vertical-align: middle; }}
            tr:nth-child(even) {{ background-color: #fafbfc; }}
            .job-title {{ font-weight: bold; color: #1a73e8; text-decoration: none; }}
            .company {{ color: #5f6368; font-size: 14px; }}
            .salary {{ font-weight: 500; color: #34a853; }}
            .skills {{ font-size: 12px; color: #666; font-style: italic; }}
            .score-high {{ background-color: #e6ffed; color: #22863a; padding: 4px 8px; border-radius: 4px; font-weight: bold; }}
            .score-medium {{ background-color: #fff8e1; color: #b78103; padding: 4px 8px; border-radius: 4px; font-weight: bold; }}
            .score-low {{ color: #666; }}
            .footer {{ text-align: center; margin-top: 30px; font-size: 12px; color: #999; border-top: 1px solid #eee; padding-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 style="margin:0;">Your Daily Job Matches</h1>
                <p style="margin:10px 0 0 0; opacity: 0.9;">Hand-picked opportunities for {date_str}</p>
            </div>
            <div class="stats">
                <div class="stat-item"><div class="stat-value">{len(jobs)}</div><div class="stat-label">Total Found</div></div>
                <div class="stat-item"><div class="stat-value">{count}</div><div class="stat-label">Top Matches</div></div>
            </div>
            <table>
                <thead><tr><th>Job Title</th><th>Company</th><th>Salary</th><th>Skills</th><th>Match</th></tr></thead>
                <tbody>
    """

    for _, job in top_jobs.iterrows():
        title = job.get('title', 'Unknown Title')
        url = job.get('job_url', '#')
        company = job.get('company', 'Unknown Company')
        salary_str = format_salary(job.get('min_amount'), job.get('max_amount'), job.get('currency', 'USD'))
        
        skills = job.get('matched_skills', [])
        skills_str = ", ".join(skills) if isinstance(skills, list) else str(skills)
            
        score = job.get('ai_match_score', 0)
        reason = job.get('ai_match_reason', '')
        score_class = "score-high" if score > 80 else ("score-medium" if score >= 60 else "score-low")
        score_display = f"{int(score)}%" if score > 0 else "N/A"

        html_content += f"""
                    <tr>
                        <td><a href="{url}" class="job-title">{title}</a><br><span style="font-size: 11px; color: #666;">{reason}</span></td>
                        <td><div class="company">{company}</div></td>
                        <td><div class="salary">{salary_str}</div></td>
                        <td><div class="skills">{skills_str}</div></td>
                        <td><span class="{score_class}">{score_display}</span></td>
                    </tr>
        """

    html_content += """
                </tbody>
            </table>
            <div class="footer"><p>Generated by <strong>JobBot</strong></p></div>
        </div>
    </body>
    </html>
    """
    msg.attach(MIMEText(html_content, 'html'))

    @retry(max_attempts=3, delay=10)
    def attempt_smtp_send():
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_address, gmail_app_password)
            server.send_message(msg)

    try:
        attempt_smtp_send()
        logger.info(f"Email digest sent successfully to {gmail_address}")
        return True
    except Exception as e:
        logger.error(f"Failed to send email after retries: {str(e)}")
        return False

def send_telegram_message(text: str, token: str, chat_id: str) -> bool:
    """Sends a message via Telegram Bot API with retry logic."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    
    @retry(max_attempts=3, delay=5)
    def post_tg():
        res = requests.post(url, json=payload, timeout=15)
        if res.status_code != 200:
            logger.error(f"Telegram API error ({res.status_code}): {res.text}")
            res.raise_for_status()
        return res

    try:
        post_tg()
        logger.info("Telegram message sent successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to send Telegram message after retries: {str(e)}")
        return False

def send_telegram_alert(jobs: pd.DataFrame, config: Dict[str, Any]):
    """Sends a Telegram alert with the top matched jobs."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logger.warning("Telegram credentials missing. Skipping Telegram alert.")
        return False

    if jobs.empty:
        return False

    date_str = datetime.now().strftime("%Y-%m-%d")
    header = f"🤖 *JobBot Daily Report* — {date_str}\n\n"
    
    job_blocks = []
    for idx, (_, job) in enumerate(jobs.head(10).iterrows(), 1):
        title = job.get('title', 'Unknown')
        company = job.get('company', 'Unknown')
        url = job.get('job_url', '#')
        score = job.get('ai_match_score', 0)
        block = f"{idx}️⃣ *{title}* ({int(score)}%)\n🏢 {company}\n🔗 [Apply Here]({url})\n\n"
        job_blocks.append(block)

    full_message = header + "".join(job_blocks)
    return send_telegram_message(full_message, token, chat_id)

def send_notifications(jobs: pd.DataFrame, config: Dict[str, Any]):
    notif_config = config.get('notifications', {})
    email_enabled = notif_config.get('email_enabled', False)
    telegram_enabled = notif_config.get('telegram_enabled', False)
    
    results = {"email_enabled": email_enabled, "telegram_enabled": telegram_enabled, "email_sent": False, "telegram_sent": False}
    
    if email_enabled:
        results["email_sent"] = send_email_digest(jobs, config)
    if telegram_enabled:
        results["telegram_sent"] = send_telegram_alert(jobs, config)
        
    return results

if __name__ == "__main__":
    from modules.logger_setup import setup_logging
    setup_logging()
    logger.info("Notifier test session.")
