import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from modules.notifier import send_notifications, send_email_digest, send_telegram_alert

@patch("smtplib.SMTP_SSL")
def test_email_digest_generation(mock_smtp, sample_jobs_df, sample_config, monkeypatch):
    """Test email digest without actual sending."""
    monkeypatch.setenv("GMAIL_ADDRESS", "test@gmail.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "mock_pw")
    
    # Mock SMTP instance
    mock_server = MagicMock()
    mock_smtp.return_value.__enter__.return_value = mock_server
    
    success = send_email_digest(sample_jobs_df, sample_config)
    
    assert success is True
    assert mock_server.login.called
    assert mock_server.send_message.called
    
    # Verify content of msg passed to send_message
    sent_msg = mock_server.send_message.call_args[0][0]
    assert "JobBot Daily Report" in sent_msg['Subject']
    # Check that sample job is in HTML parts
    html_body = sent_msg.get_payload(0).get_payload()
    assert "Tech Solutions" in html_body

@patch("requests.post")
def test_telegram_alert_formatting(mock_post, sample_jobs_df, sample_config, monkeypatch):
    """Test Telegram message formatting and delivery mock."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "mock_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "mock_id")
    
    mock_post.return_value.status_code = 200
    
    success = send_telegram_alert(sample_jobs_df, sample_config)
    
    assert success is True
    assert mock_post.called
    
    # Verify payload
    payload = mock_post.call_args[1]['json']
    assert "🤖 *JobBot Daily Report*" in payload['text']
    assert "Tech Solutions" in payload['text']

def test_notifier_missing_creds(sample_jobs_df, sample_config, monkeypatch):
    """Ensure skipping notifications when credentials are missing."""
    monkeypatch.delenv("GMAIL_ADDRESS", raising=False)
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    
    results = send_notifications(sample_jobs_df, sample_config)
    
    assert results["email_sent"] is False
    assert results["telegram_sent"] is False

@patch("modules.notifier.send_email_digest")
@patch("modules.notifier.send_telegram_alert")
def test_send_notifications_orchestration(mock_tg, mock_email, sample_jobs_df, sample_config):
    """Test that send_notifications calls both based on config."""
    sample_config["notifications"]["email_enabled"] = True
    sample_config["notifications"]["telegram_enabled"] = True
    
    send_notifications(sample_jobs_df, sample_config)
    
    assert mock_email.called
    assert mock_tg.called
