import logging
from config import get_config

# Set up logging for main
logger = logging.getLogger("JobBot.Main")

def main():
    """
    Main entry point for JobBot.
    Loads configuration and prepares the modules for execution.
    """
    logger.info("Starting JobBot...")
    
    try:
        # Load configuration
        config = get_config()
        
        # Display search parameters
        search_terms = config.get("search_terms", [])
        logger.info(f"Search terms: {search_terms}")
        logger.info(f"Target country: {config.get('country')}")
        logger.info(f"Target job type: {config.get('job_type')}")
        
        # Phase 0 ends here (successful config loading)
        logger.info("Phase 0 complete: Project structure and configuration established.")
        
    except Exception as e:
        logger.critical(f"A critical error occurred: {e}")

if __name__ == "__main__":
    main()
