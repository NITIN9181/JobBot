import pytest
import os
import yaml
import sys
from config import validate_config, get_config

def test_validate_config_valid(sample_config):
    """Test that a valid config passes validation without error."""
    # Should not raise any exception
    validate_config(sample_config)

def test_validate_config_missing_field(sample_config):
    """Test that missing required fields trigger SystemExit."""
    invalid_config = sample_config.copy()
    del invalid_config["search_terms"]
    
    with pytest.raises(SystemExit) as excinfo:
        validate_config(invalid_config)
    assert excinfo.value.code == 1

def test_validate_config_wrong_type(sample_config):
    """Test that incorrect types trigger SystemExit."""
    invalid_config = sample_config.copy()
    invalid_config["min_salary"] = "high"  # Should be int
    
    with pytest.raises(SystemExit) as excinfo:
        validate_config(invalid_config)
    assert excinfo.value.code == 1

def test_validate_config_empty_search(sample_config):
    """Test that empty search terms list triggers SystemExit."""
    invalid_config = sample_config.copy()
    invalid_config["search_terms"] = []
    
    with pytest.raises(SystemExit) as excinfo:
        validate_config(invalid_config)
    assert excinfo.value.code == 1

def test_get_config_success(tmp_path, monkeypatch, mock_env):
    """Test full config loading including .env merging."""
    # Create a temporary config.yaml
    d = tmp_path / "config_test"
    d.mkdir()
    config_file = d / "config.yaml"
    
    test_yaml = {
        "search_terms": ["Python"],
        "skills": ["python"],
        "min_salary": 50000,
        "job_type": "full-time",
        "country": "USA",
        "results_per_site": 10,
        "hours_old": 24
    }
    
    with open(config_file, "w") as f:
        yaml.dump(test_yaml, f)
        
    # Change CWD to the temp path for get_config to find config.yaml
    monkeypatch.chdir(d)
    
    # We also need to mock os.path.exists and open if we don't want to rely on chdir
    # but chdir is cleaner for this test.
    
    config = get_config()
    
    assert config["search_terms"] == ["Python"]
    assert config["groq_api_key"] == "mock_groq_key"
    assert config["nvidia_api_key"] == "mock_nvidia_key"
    assert config["gmail_address"] == "test@gmail.com"
