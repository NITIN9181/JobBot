import pytest
import pandas as pd
import json
from unittest.mock import patch, MagicMock
from modules.scorer import create_user_profile, score_all_jobs, score_single_job

def test_create_user_profile(sample_config):
    """Test building of user profile string from config."""
    profile = create_user_profile(sample_config)
    assert "Python Developer" in profile
    assert "80000" in profile
    assert "full-time" in profile

@patch("modules.scorer.load_score_cache")
@patch("modules.scorer.OpenAI")
def test_score_all_jobs_success(mock_openai, mock_cache, sample_jobs_df, sample_config, monkeypatch):
    """Test full scoring workflow with mocked AI client."""
    monkeypatch.setenv("NVIDIA_API_KEY", "mock_key")
    mock_cache.return_value = {} # Ensure fresh cache
    
    # Mock client and response
    mock_client = MagicMock()
    mock_openai.return_value = mock_client
    
    mock_response = MagicMock()
    mock_response.choices[0].message.content = json.dumps({
        "score": 85,
        "reason": "Great match for Python and AWS",
        "key_matches": ["python", "aws"],
        "missing_skills": []
    })
    mock_client.chat.completions.create.return_value = mock_response
    
    # Run scoring - limiting max jobs to speed up
    sample_config["ai_scoring"]["max_jobs_to_score"] = 1
    scored_df, stats = score_all_jobs(sample_jobs_df.head(1), sample_config)
    
    assert not scored_df.empty
    assert scored_df.iloc[0]["ai_match_score"] == 85
    assert stats["enabled"] is True
    assert stats["new"] == 1

def test_score_all_jobs_no_api_key(sample_jobs_df, sample_config, monkeypatch):
    """Test graceful skip when API key is missing."""
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    
    scored_df, stats = score_all_jobs(sample_jobs_df, sample_config)
    
    assert stats["enabled"] is False
    assert "ai_match_score" in scored_df.columns
    assert scored_df["ai_match_score"].isna().all()

@patch("modules.scorer.load_score_cache")
@patch("modules.scorer.OpenAI")
def test_malformed_ai_response(mock_openai, mock_cache, sample_jobs_df, sample_config, monkeypatch):
    """Test fallback when AI returns invalid JSON."""
    monkeypatch.setenv("NVIDIA_API_KEY", "mock_key")
    mock_cache.return_value = {} # Ensure fresh cache
    
    mock_client = MagicMock()
    mock_openai.return_value = mock_client
    
    mock_response = MagicMock()
    mock_response.choices[0].message.content = "Invalid JSON response"
    mock_client.chat.completions.create.return_value = mock_response
    
    sample_config["ai_scoring"]["max_jobs_to_score"] = 1
    scored_df, stats = score_all_jobs(sample_jobs_df.head(1), sample_config)
    
    # Should fallback to default score (usually 50)
    assert scored_df.iloc[0]["ai_match_score"] == 50
    assert "Could not analyze" in scored_df.iloc[0]["ai_match_reason"]
