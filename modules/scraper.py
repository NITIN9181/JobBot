import pandas as pd
import logging
import time
from typing import List, Dict, Any, Optional
from jobspy import scrape_jobs
from modules.utils import retry

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)


def _build_search_terms(base_terms: List[str], target_country: str) -> List[str]:
    """
    Expands the base search term list based on the target country.

    For India, each term is tripled by also searching with ' fresher' and
    ' entry level' appended — this catches India-specific postings that use
    those exact phrases in the job title.

    Args:
        base_terms: The raw list of search terms from config.
        target_country: The target country string (e.g. "India", "any").

    Returns:
        Expanded list of search terms.
    """
    if target_country.lower() == "india":
        expanded: List[str] = []
        for term in base_terms:
            expanded.append(term)
            expanded.append(f"{term} fresher")
            expanded.append(f"{term} entry level")
        logger.info(
            "India mode: expanded %d base terms to %d terms "
            "(added ' fresher' and ' entry level' variants).",
            len(base_terms),
            len(expanded),
        )
        return expanded
    return base_terms


def _country_indeed_param(target_country: str) -> Optional[str]:
    """
    Returns the country_indeed parameter value for the jobspy scrape_jobs() call.

    Args:
        target_country: The target country string.

    Returns:
        The country_indeed string, or None if any country is acceptable.
    """
    if target_country.lower() == "any":
        return None
    return target_country


def scrape_all_jobs(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Scrapes jobs from multiple job boards based on search terms in the configuration.

    Reads ``target_country`` from config (defaults to "India") and adjusts
    jobspy parameters accordingly:

    - **India**: Sets ``country_indeed="India"``, triples search terms by
      appending ``" fresher"`` and ``" entry level"`` variants.
    - **any**: Skips the ``country_indeed`` parameter entirely.
    - **Other**: Uses the value verbatim as ``country_indeed``.

    Each result row also gets a ``source_platform`` column (e.g. "JobSpy-indeed")
    to make downstream source tracking easy.

    Args:
        config: A dictionary containing search parameters:
            - search_terms (List[str]): Titles to search for.
            - results_per_site (int): Max results per job board.
            - hours_old (int): Max age of job postings in hours.
            - country (str): Legacy country code (kept for backward compat).
            - target_country (str): Preferred country for eligibility filtering.

    Returns:
        pd.DataFrame: A combined DataFrame containing all found job listings.
    """
    base_terms: List[str] = config.get("search_terms", [])
    results_per_site: int = config.get("results_per_site", 50)
    hours_old: int = config.get("hours_old", 24)
    target_country: str = config.get("target_country", "India")

    country_indeed: Optional[str] = _country_indeed_param(target_country)
    search_terms: List[str] = _build_search_terms(base_terms, target_country)

    logger.info("Scraping AI/ML/SDE jobs for country: %s", target_country)

    # Supported sites in python-jobspy — refined for stability
    sites = ["indeed", "linkedin", "google"]

    all_results: List[pd.DataFrame] = []

    logger.info(
        "Starting job scraping sequence for %d search terms (%d base × expansion).",
        len(search_terms),
        len(base_terms),
    )

    for term in search_terms:
        logger.info("Scraping for search term: '%s'", term)
        try:
            @retry(max_attempts=3, delay=10)
            def fetch_board_data(
                _term: str = term,
                _country_indeed: Optional[str] = country_indeed,
            ) -> pd.DataFrame:
                kwargs: Dict[str, Any] = dict(
                    site_name=sites,
                    search_term=_term,
                    location="Remote",
                    is_remote=True,
                    results_wanted=results_per_site,
                    hours_old=hours_old,
                )
                if _country_indeed is not None:
                    kwargs["country_indeed"] = _country_indeed
                return scrape_jobs(**kwargs)

            jobs: pd.DataFrame = fetch_board_data()

            if not jobs.empty:
                # Track which search term produced each row
                jobs["source_search_term"] = term

                # Add source_platform column: "JobSpy-{site}" per row
                # jobspy stores the originating site in a 'site' column when available
                if "site" in jobs.columns:
                    jobs["source_platform"] = "JobSpy-" + jobs["site"].astype(str)
                else:
                    jobs["source_platform"] = "JobSpy-unknown"

                all_results.append(jobs)
                logger.info("Successfully found %d jobs for '%s'.", len(jobs), term)
            else:
                logger.info("No jobs found for search term: '%s'.", term)

        except Exception as e:
            logger.error("Error encountered while scraping for '%s': %s", term, str(e))

        # Delay between each search term to avoid rate limiting
        logger.debug("Waiting 3 seconds before next search...")
        time.sleep(3)

    if not all_results:
        logger.warning("No jobs were found across all search terms and sites.")
        return pd.DataFrame()

    # Combine all individual results into a single DataFrame
    combined_df = pd.concat(all_results, ignore_index=True)

    logger.info("Total jobs found across all searches: %d", len(combined_df))

    return combined_df


if __name__ == "__main__":
    # Standalone test block
    from modules.logger_setup import setup_logging

    setup_logging()

    test_config = {
        "search_terms": ["ML Engineer"],
        "results_per_site": 2,
        "hours_old": 72,
        "target_country": "India",
    }

    logger.info("Starting Scraper auto-verification (India mode)...")

    try:
        results = scrape_all_jobs(test_config)
        logger.info("Verification complete. Found %d jobs.", len(results))
        if not results.empty:
            cols = ["title", "company", "source_search_term", "source_platform"]
            available = [c for c in cols if c in results.columns]
            print(results[available].head(5).to_string(index=False))
    except Exception as e:
        logger.error("Verification failed: %s", e)
