import pytest
import pandas as pd
import os
from unittest.mock import MagicMock

@pytest.fixture
def sample_config():
    """Returns a standard valid configuration dictionary."""
    return {
        "search_terms": ["Python Developer", "Data Scientist"],
        "skills": ["python", "aws", "docker"],
        "min_salary": 80000,
        "job_type": "full-time",
        "country": "USA",
        "results_per_site": 10,
        "hours_old": 48,
        "blacklisted_companies": ["Evil Corp"],
        "ai_scoring": {
            "enabled": True,
            "min_score": 70,
            "max_jobs_to_score": 10
        },
        "notifications": {
            "email_enabled": True,
            "telegram_enabled": True
        }
    }

@pytest.fixture
def sample_jobs_df():
    """Returns a sample pandas DataFrame with diverse job entries."""
    data = [
        {
            "title": "Senior Python Developer",
            "company": "Tech Solutions",
            "location": "Remote",
            "job_url": "https://example.com/job1",
            "job_type": "full-time",
            "min_amount": 90000,
            "max_amount": 120000,
            "currency": "USD",
            "date_posted": "2024-04-10",
            "description": "We need an AWS and Python expert.",
            "source_board": "linkedin",
            "skill_match_count": 2
        },
        {
            "title": "Junior Data Scientist",
            "company": "Data-ize",
            "location": "New York (Remote)",
            "job_url": "https://example.com/job2",
            "job_type": "contract",
            "min_amount": 60000,
            "max_amount": 75000,
            "currency": "USD",
            "date_posted": "2024-04-11",
            "description": "SQL and Python required in this Docker environment.",
            "source_board": "indeed",
            "skill_match_count": 2
        },
        {
            "title": "Software Engineer",
            "company": "Evil Corp",
            "location": "Remote",
            "job_url": "https://example.com/job3",
            "job_type": "full-time",
            "min_amount": 100000,
            "max_amount": 150000,
            "currency": "USD",
            "date_posted": "2024-04-12",
            "description": "Python developer wanted.",
            "source_board": "google",
            "skill_match_count": 1
        }
    ]
    return pd.DataFrame(data)

@pytest.fixture
def mock_env(monkeypatch):
    """Mocks common environment variables."""
    monkeypatch.setenv("GROQ_API_KEY", "mock_groq_key")
    monkeypatch.setenv("NVIDIA_API_KEY", "mock_nvidia_key")
    monkeypatch.setenv("GMAIL_ADDRESS", "test@gmail.com")
    monkeypatch.setenv("GMAIL_APP_PASSWORD", "mock_app_pw")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "mock_tg_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345678")
    monkeypatch.setenv("GOOGLE_SHEETS_CRED_FILE", "mock_creds.json")
    monkeypatch.setenv("GOOGLE_SHEET_NAME", "MockSheet")
