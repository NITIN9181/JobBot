import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

def setup_logging(console_level=logging.INFO, file_level=logging.DEBUG):
    """
    Configures a centralized logger that writes to both console and a rotating file.
    
    Args:
        console_level: Logging level for the terminal (default: INFO)
        file_level: Logging level for the file (default: DEBUG)
    """
    # Ensure logs directory exists
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Base log filename: jobbot_YYYY-MM-DD.log
    # But TimedRotatingFileHandler will append suffixes, so we start with a generic one
    log_filename = os.path.join(log_dir, "jobbot.log")
    
    # Formatter for Console
    console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s — %(message)s', datefmt='%H:%M:%S')
    
    # Formatter for File
    file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s — %(name)s — %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG) # Catch everything, handlers will filter
    
    # Clean up any existing handlers (prevent duplicates)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(console_level)
    root_logger.addHandler(console_handler)
    
    # File Handler (Rotating every midnight, keeping 7 days)
    file_handler = TimedRotatingFileHandler(
        log_filename,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8"
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(file_level)
    root_logger.addHandler(file_handler)
    
    logging.getLogger("JobSpy").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    
    return root_logger

if __name__ == "__main__":
    # Test logger
    logger = setup_logging()
    logger.info("Test INFO message for console and file")
    logger.debug("Test DEBUG message for file ONLY")
    logger.error("Test ERROR message for both")
