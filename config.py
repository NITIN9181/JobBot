import os
import yaml
import logging
from typing import Any, Dict
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_config() -> Dict[str, Any]:
    """
    Loads configuration from config.yaml and environments variables from .env.
    Validates required fields and returns a unified configuration dictionary.
    """
    # Load .env file
    load_dotenv()
    
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        logger.error(f"Configuration file {config_path} not found.")
        raise FileNotFoundError(f"Missing {config_path}")

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error parsing {config_path}: {e}")
        raise

    # Validation of required fields
    required_fields = ["search_terms", "skills", "job_type", "country"]
    missing_fields = [field for field in required_fields if field not in config or not config[field]]
    
    if missing_fields:
        error_msg = f"Missing required configuration fields: {', '.join(missing_fields)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Load sensitive data from environment
    config['groq_api_key'] = os.getenv("GROQ_API_KEY")
    config['gmail_address'] = os.getenv("GMAIL_ADDRESS")
    config['gmail_app_password'] = os.getenv("GMAIL_APP_PASSWORD")
    config['telegram_bot_token'] = os.getenv("TELEGRAM_BOT_TOKEN")
    config['telegram_chat_id'] = os.getenv("TELEGRAM_CHAT_ID")
    config['google_sheets_cred_file'] = os.getenv("GOOGLE_SHEETS_CRED_FILE")
    config['google_sheet_name'] = os.getenv("GOOGLE_SHEET_NAME")

    logger.info("Configuration loaded successfully.")
    return config

if __name__ == "__main__":
    # Test loading
    try:
        cfg = get_config()
        print("Config loaded:")
        for key, value in cfg.items():
            if "key" in key or "password" in key:
                print(f"  {key}: ********")
            else:
                print(f"  {key}: {value}")
    except Exception as e:
        print(f"Failed to load config: {e}")
