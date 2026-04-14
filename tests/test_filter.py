import pytest
import pandas as pd
from modules.filter_engine import filter_jobs, remove_duplicates, sort_jobs

def test_skills_matching(sample_jobs_df, sample_config):
    """Test that skills matching is case-insensitive and identifies keywords."""
    # Modify one job to have specific skills in description
    sample_jobs_df.loc[0, 'description'] = "Looking for a Python and AWS expert."
    sample_config['skills'] = ['PYTHON', 'AWS', 'nonexistent']
    
    filtered = filter_jobs(sample_jobs_df, sample_config)
    
    # All sampled jobs have 'python' in description or title
    assert not filtered.empty
    assert 'matched_skills' in filtered.columns
    assert 'python' in [s.lower() for s in filtered.iloc[0]['matched_skills']]
    assert 'aws' in [s.lower() for s in filtered.iloc[0]['matched_skills']]

def test_salary_filtering(sample_jobs_df, sample_config):
    """Test salary threshold logic."""
    sample_config['min_salary'] = 110000
    # Job 1: 90k-120k (Pass because max >= 110k)
    # Job 2: 60k-75k (Fail)
    # Job 3: 100k-150k (Pass because max >= 110k)
    
    # We need to temporarily disable other filters like blacklist
    sample_config['blacklisted_companies'] = []
    
    filtered = filter_jobs(sample_jobs_df, sample_config)
    
    assert len(filtered) == 2
    assert "Tech Solutions" in filtered['company'].values
    assert "Evil Corp" in filtered['company'].values

def test_job_type_filtering(sample_jobs_df, sample_config):
    """Test filtering by job type (full-time, contract, etc.)."""
    sample_config['job_type'] = "contract"
    sample_config['min_salary'] = 0
    sample_config['blacklisted_companies'] = []
    
    filtered = filter_jobs(sample_jobs_df, sample_config)
    
    assert len(filtered) == 1
    assert filtered.iloc[0]['job_type'] == "contract"

def test_blacklist_filtering(sample_jobs_df, sample_config):
    """Test that companies in blacklist are excluded."""
    sample_config['blacklisted_companies'] = ["Evil Corp"]
    sample_config['min_salary'] = 0
    
    filtered = filter_jobs(sample_jobs_df, sample_config)
    
    assert "Evil Corp" not in filtered['company'].values

def test_remove_duplicates():
    """Test removal of duplicate jobs by URL and Title|Company."""
    data = [
        {"title": "Dev", "company": "A", "job_url": "url1", "location": "Remote"},
        {"title": "Dev", "company": "A", "job_url": "url2", "location": "Remote"}, # Duplicate title|company
        {"title": "Dev 2", "company": "B", "job_url": "url1", "location": "Remote"}, # Duplicate URL
    ]
    df = pd.DataFrame(data)
    
    deduped = remove_duplicates(df)
    
    assert len(deduped) == 1 # Only the first one should remain if both URL and Title|Company collide across the set

def test_filter_sequence(sample_jobs_df, sample_config):
    """Test that all filters work together correctly."""
    # Set config to match exactly one job (Job 1)
    sample_config['skills'] = ['aws']
    sample_config['min_salary'] = 90000
    sample_config['job_type'] = 'full-time'
    sample_config['blacklisted_companies'] = ['Evil Corp']
    
    filtered = filter_jobs(sample_jobs_df, sample_config)
    
    assert len(filtered) == 1
    assert filtered.iloc[0]['company'] == "Tech Solutions"
