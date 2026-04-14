import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from modules.scraper import scrape_all_jobs

@pytest.fixture
def mock_scrape_jobs():
    with patch("modules.scraper.scrape_jobs") as mock:
        yield mock

@pytest.fixture
def mock_sleep():
    with patch("time.sleep") as mock:
        yield mock

def test_scrape_all_jobs_returns_df(sample_config, mock_scrape_jobs, mock_sleep):
    """Test that scrape_all_jobs returns a DataFrame with combined results."""
    # Mock data for two search terms
    df1 = pd.DataFrame([{"title": "Job 1", "company": "A"}])
    df2 = pd.DataFrame([{"title": "Job 2", "company": "B"}])
    
    # Side effect: first call returns df1, second returns df2
    mock_scrape_jobs.side_effect = [df1, df2]
    
    results = scrape_all_jobs(sample_config)
    
    assert isinstance(results, pd.DataFrame)
    assert len(results) == 2
    assert "source_search_term" in results.columns
    # Check that sleep was called between searches
    assert mock_sleep.call_count == len(sample_config["search_terms"])

def test_scrape_all_jobs_empty(sample_config, mock_scrape_jobs, mock_sleep):
    """Test handling of no jobs found."""
    mock_scrape_jobs.return_value = pd.DataFrame()
    
    results = scrape_all_jobs(sample_config)
    
    assert results.empty
    assert isinstance(results, pd.DataFrame)

def test_scrape_all_jobs_one_site_fails(sample_config, mock_scrape_jobs, mock_sleep):
    """Test that it continues when one search term/fetch fails."""
    df1 = pd.DataFrame([{"title": "Job 1", "company": "A"}])
    
    # Side effect: first call succeeds, second raises exception
    mock_scrape_jobs.side_effect = [df1, Exception("API Error")]
    
    results = scrape_all_jobs(sample_config)
    
    # Should still contain the first job
    assert len(results) == 1
    assert results.iloc[0]["title"] == "Job 1"

def test_scrape_all_jobs_delay(sample_config, mock_scrape_jobs, mock_sleep):
    """Test that 3-second delay is applied between searches."""
    scrape_all_jobs(sample_config)
    
    # Should be called for each search term
    assert mock_sleep.call_count == len(sample_config["search_terms"])
    mock_sleep.assert_called_with(3)
