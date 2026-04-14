import pytest
import pandas as pd
import os
from modules.deduplicator import deduplicate_with_history, get_history_stats, update_history

def test_first_run_no_history(sample_jobs_df, tmp_path):
    """Test that on the first run, all jobs are considered new and history is created."""
    history_file = tmp_path / "job_history.csv"
    
    # Run deduplication
    new_jobs = deduplicate_with_history(sample_jobs_df, history_file=str(history_file))
    
    assert len(new_jobs) == len(sample_jobs_df)
    assert os.path.exists(history_file)
    
    # Verify content of history file
    history_df = pd.read_csv(history_file)
    assert len(history_df) == len(sample_jobs_df)
    assert "first_seen_date" in history_df.columns

def test_duplicate_detection(sample_jobs_df, tmp_path):
    """Test that already seen jobs are filtered out."""
    history_file = tmp_path / "job_history.csv"
    
    # First run
    deduplicate_with_history(sample_jobs_df, history_file=str(history_file))
    
    # Second run with same jobs + 1 new job
    new_job = pd.DataFrame([{
        "title": "New Unique Job",
        "company": "Startup Inc",
        "job_url": "https://example.com/unique",
        "location": "Remote"
    }])
    combined_df = pd.concat([sample_jobs_df, new_job], ignore_index=True)
    
    results = deduplicate_with_history(combined_df, history_file=str(history_file))
    
    assert len(results) == 1
    assert results.iloc[0]["title"] == "New Unique Job"

def test_history_update(sample_jobs_df, tmp_path):
    """Test that history file appends correctly."""
    history_file = tmp_path / "job_history.csv"
    
    # Add initial batch
    update_history(sample_jobs_df.head(1), history_file=str(history_file))
    assert len(pd.read_csv(history_file)) == 1
    
    # Add second batch
    update_history(sample_jobs_df.tail(1), history_file=str(history_file))
    assert len(pd.read_csv(history_file)) == 2

def test_get_history_stats(sample_jobs_df, tmp_path):
    """Test stats calculation from historical data."""
    history_file = tmp_path / "job_history.csv"
    
    # Setup history with known data
    update_history(sample_jobs_df, history_file=str(history_file))
    
    stats = get_history_stats(history_file=str(history_file))
    
    assert stats["total_seen"] == len(sample_jobs_df)
    assert stats["today"] == len(sample_jobs_df) # Since they were just added
    assert "Tech Solutions" in stats["top_companies"]
