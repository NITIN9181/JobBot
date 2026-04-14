import pandas as pd
import logging
import time
from typing import List, Dict, Any
from jobspy import scrape_jobs
from modules.utils import retry

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

def scrape_all_jobs(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Scrapes jobs from multiple job boards based on search terms in the configuration.
    
    Args:
        config (Dict[str, Any]): A dictionary containing search parameters:
            - search_terms (List[str]): Titles to search for.
            - results_per_site (int): Max results per job board.
            - hours_old (int): Max age of job postings in hours.
            - country (str): Country code for the search (e.g., "USA").
            
    Returns:
        pd.DataFrame: A combined DataFrame containing all found job listings.
    """
    search_terms = config.get("search_terms", [])
    results_per_site = config.get("results_per_site", 50)
    hours_old = config.get("hours_old", 24)
    country = config.get("country", "USA")
    
    # Supported sites in python-jobspy - refined for stability
    sites = ["indeed", "linkedin", "google", "zip_recruiter", "glassdoor"]
    
    all_results = []
    
    logger.info(f"Starting job scraping sequence for {len(search_terms)} terms.")
    
    for term in search_terms:
        logger.info(f"Scraping for search term: '{term}'")
        try:
            # Scrape jobs using jobspy — wrapped in a local retry-able call if needed, 
            # but here we'll just use the try/except and maybe add a retry helper.
            
            @retry(max_attempts=3, delay=10)
            def fetch_board_data():
                return scrape_jobs(
                    site_name=sites,
                    search_term=term,
                    location="Remote",
                    is_remote=True,
                    results_wanted=results_per_site,
                    hours_old=hours_old,
                    country_indeed=country
                )
            
            jobs = fetch_board_data()
            
            if not jobs.empty:
                # Add a column to track which search term found each job
                jobs["source_search_term"] = term
                all_results.append(jobs)
                logger.info(f"Successfully found {len(jobs)} jobs for '{term}'.")
            else:
                logger.info(f"No jobs found for search term: '{term}'.")
                
        except Exception as e:
            logger.error(f"Error encountered while scraping for '{term}': {str(e)}")
            
        # Add a delay between each search term to avoid rate limiting
        logger.debug("Waiting 3 seconds before next search...")
        time.sleep(3)
        
    if not all_results:
        logger.warning("No jobs were found across all search terms and sites.")
        return pd.DataFrame()
    
    # Combine all individual results into a single DataFrame
    combined_df = pd.concat(all_results, ignore_index=True)
    
    # Basic logging of final counts
    logger.info(f"Total jobs found across all searches: {len(combined_df)}")
    
    return combined_df

if __name__ == "__main__":
    # Standalone test block
    from modules.logger_setup import setup_logging
    setup_logging()
    
    test_config = {
        "search_terms": ["Python Developer"],
        "results_per_site": 2,
        "hours_old": 24,
        "country": "USA"
    }
    
    logger.info("Starting Scraper auto-verification...")
    
    try:
        results = scrape_all_jobs(test_config)
        logger.info(f"Verification complete. Found {len(results)} jobs.")
    except Exception as e:
        logger.error(f"Verification failed: {e}")
