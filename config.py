import os
import yaml
import logging
import sys
from typing import Any, Dict, List
from dotenv import load_dotenv
from modules.logger_setup import setup_logging

# Initialize logging
logger = setup_logging()

def validate_config(config: Dict[str, Any]):
    """
    Validates configuration values with helpful error messages.
    """
    errors = []
    
    # Structure of rules: (key, expected_type, helpful_fix)
    rules = [
        ("search_terms", list, "Set search_terms to a list like ['Python Developer']"),
        ("skills", list, "Set skills to a list like ['python', 'aws']"),
        ("min_salary", int, "Set min_salary to a number like 60000"),
        ("job_type", str, "Set job_type to a string like 'full-time'"),
        ("country", str, "Set country to a country code like 'USA' or 'UK'"),
        ("results_per_site", int, "Set results_per_site to a number like 50"),
        ("hours_old", int, "Set hours_old to a number like 24"),
    ]
    
    for key, expected_type, fix in rules:
        val = config.get(key)
        if val is None:
            errors.append(f"❌ Config Error: Missing required field '{key}'\n💡 Fix: {fix}")
        elif not isinstance(val, expected_type):
            errors.append(f"❌ Config Error: '{key}' must be a {expected_type.__name__}, got '{type(val).__name__}'\n💡 Fix: {fix}")
            
    # Check if empty lists
    if isinstance(config.get("search_terms"), list) and not config.get("search_terms"):
        errors.append("❌ Config Error: 'search_terms' list is empty\n💡 Fix: Add at least one job title to search for")
        
    if errors:
        print("\n" + "!"*40)
        print("   CONFIGURATION VALIDATION FAILED")
        print("!"*40)
        for err in errors:
            print(f"\n{err}")
        print("\n" + "!"*40)
        sys.exit(1)

def get_config() -> Dict[str, Any]:
    """
    Loads configuration from config.yaml and environments variables from .env.
    Validates required fields and returns a unified configuration dictionary.
    """
    # Load .env file
    load_dotenv()
    
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        logger.critical(f"Configuration file {config_path} not found.")
        print(f"❌ Critical Error: Missing {config_path}")
        sys.exit(1)

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.critical(f"Error parsing {config_path}: {e}")
        print(f"❌ Error Parsing {config_path}: {e}")
        sys.exit(1)

    # Perform thorough validation
    validate_config(config)

    # Load sensitive data from environment
    config['groq_api_key'] = os.getenv("GROQ_API_KEY")
    config['nvidia_api_key'] = os.getenv("NVIDIA_API_KEY")
    config['gmail_address'] = os.getenv("GMAIL_ADDRESS")
    config['gmail_app_password'] = os.getenv("GMAIL_APP_PASSWORD")
    config['telegram_bot_token'] = os.getenv("TELEGRAM_BOT_TOKEN")
    config['telegram_chat_id'] = os.getenv("TELEGRAM_CHAT_ID")
    config['google_sheets_cred_file'] = os.getenv("GOOGLE_SHEETS_CRED_FILE")
    config['google_sheet_name'] = os.getenv("GOOGLE_SHEET_NAME")

    logger.info("Configuration loaded successfully")
    return config

if __name__ == "__main__":
    # Test loading
    try:
        cfg = get_config()
        print("\nConfig loaded successfully.")
    except Exception as e:
        print(f"Failed to load config: {e}")
