import pandas as pd
import re
import logging
from typing import List, Dict, Any, Optional

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

def extract_experience_requirement(text: str) -> dict:
    """
    Parses the text to extract structured experience data using regex patterns.
    Returns a dictionary with level, min_years, max_years, signals, and raw_match.
    """
    if not isinstance(text, str) or not text:
        return {"level": "unknown", "min_years": None, "max_years": None, "signals": [], "raw_match": None}

    text_lower = text.lower()
    
    # Define regex patterns
    patterns = {
        "range": r'(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)',
        "min_plus": r'(\d+)\+?\s*(?:years?|yrs?)',
        "explicit_min": r'(?:at least|minimum|min)\s*(\d+)\s*(?:years?|yrs?)'
    }
    
    signals = []
    entry_keywords = ["junior", "entry level", "entry-level", "fresher", "new grad", "intern", "trainee", "apprentice"]
    for kw in entry_keywords:
        if kw in text_lower:
            signals.append(kw)
            
    res = {"level": "unknown", "min_years": None, "max_years": None, "signals": signals, "raw_match": None}

    # Extract years
    match_range = re.search(patterns["range"], text_lower)
    match_min_plus = re.search(patterns["min_plus"], text_lower)
    match_explicit_min = re.search(patterns["explicit_min"], text_lower)

    if match_range:
        res["min_years"] = int(match_range.group(1))
        res["max_years"] = int(match_range.group(2))
        res["raw_match"] = match_range.group(0)
    elif match_explicit_min:
        res["min_years"] = int(match_explicit_min.group(1))
        res["raw_match"] = match_explicit_min.group(0)
    elif match_min_plus:
        res["min_years"] = int(match_min_plus.group(1))
        res["raw_match"] = match_min_plus.group(0)

    # Map to level
    min_yrs = res["min_years"]
    if min_yrs is not None:
        if min_yrs <= 1:
            res["level"] = "entry"
        elif 2 <= min_yrs <= 4:
            res["level"] = "mid"
        else:
            res["level"] = "senior"
    elif signals:
        res["level"] = "entry"
    
    return res

def filter_india_eligible(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identifies jobs open to candidates based in India.
    Checks location and description for eligibility and exclusion signals.
    """
    if df.empty: return df
    
    initial_count = len(df)
    
    eligible_loc_keywords = ["india", "worldwide", "global", "anywhere", "remote", "apac", "asia", "asia-pacific"]
    eligible_desc_keywords = [
        "open to candidates from india", "india", "apac region", 
        "globally distributed", "anywhere in the world", "open to all locations"
    ]
    
    exclusion_keywords = [
        "us only", "usa only", "united states only", "u.s. only",
        "eu only", "europe only", "uk only", "canada only", "australia only",
        "citizenship required", "work authorization required", "security clearance",
        "no international", "domestic only"
    ]

    def check_row(row):
        loc = str(row.get('location', '')).lower()
        desc = str(row.get('description', '')).lower()
        
        # 1. Check for explicit exclusions
        for exc in exclusion_keywords:
            if exc in loc or exc in desc:
                return False
        
        # 2. Check for "must be based in" followed by non-India
        # Simple heuristic: if 'must be based in' exists but 'india' doesn't follow soon
        if "must be based in" in loc or "must be based in" in desc:
            if "india" not in loc and "india" not in desc:
                return False
        if "must reside in" in loc or "must reside in" in desc:
            if "india" not in loc and "india" not in desc:
                return False

        # 3. Check for eligibility signals
        is_loc_eligible = any(kw in loc for kw in eligible_loc_keywords)
        is_desc_eligible = any(kw in desc for kw in eligible_desc_keywords)
        
        if is_loc_eligible or is_desc_eligible:
            return True
        
        # 4. Ambiguous case: location empty and no exclusions
        if not loc.strip() or loc == 'nan':
            return True
            
        # Benefit of the doubt for ambiguous cases
        return True

    df = df.copy()
    df['india_eligible'] = df.apply(check_row, axis=1)
    
    kept_df = df[df['india_eligible']].copy()
    rejected = initial_count - len(kept_df)
    
    logger.info(f"India Filter: {len(kept_df)}/{initial_count} jobs are India-eligible ({rejected} rejected)")
    
    if rejected > 0:
        rejected_examples = df[~df['india_eligible']].head(3)
        for _, r in rejected_examples.iterrows():
            logger.debug(f"Rejected (India): {r['title']} at {r['company']} (Location: {r['location']})")
            
    return kept_df

def filter_fresher_friendly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters for jobs suitable for freshers (<1 year experience).
    """
    if df.empty: return df
    
    initial_count = len(df)
    
    fresher_titles = ["junior", "fresher", "new grad", "graduate", "intern", "trainee", "associate", "apprentice"]
    senior_titles = ["senior", "sr.", "lead", "principal", "staff", "architect", "director", "vp", "head of", "manager"]
    
    fresher_desc_signals = ["entry level", "entry-level", "0-1 year", "0-2 years", "1 year", "no experience", "fresh graduate"]
    mid_senior_desc_signals = ["extensive experience", "proven track record", "seasoned professional"]

    def check_row(row):
        title = str(row.get('title', '')).lower()
        desc = str(row.get('description', '')).lower()
        
        # Benefit of the doubt for short or missing descriptions
        if len(desc) < 50 or not desc.strip():
            return True

        # 1. Title Checks
        # If title says Senior/Lead, generally reject unless it also says Junior (rare)
        is_senior_title = any(f" {st} " in f" {title} " or title.startswith(st) for st in senior_titles)
        is_junior_title = any(jt in title for jt in fresher_titles)
        
        if is_senior_title and not is_junior_title:
            # Check if description softens it: "senior or equivalent"
            if "or equivalent experience" not in desc:
                return False

        # 2. Extract Experience
        exp = extract_experience_requirement(desc)
        if exp["min_years"] is not None and exp["min_years"] >= 3:
            return False
        
        # 3. Description Keyword Checks
        if any(sig in desc for sig in mid_senior_desc_signals):
            return False
            
        if any(sig in desc for sig in fresher_desc_signals) or is_junior_title:
            return True
            
        # Ambiguous: Keep it
        return True

    df = df.copy()
    df['fresher_friendly'] = df.apply(check_row, axis=1)
    
    kept_df = df[df['fresher_friendly']].copy()
    rejected = initial_count - len(kept_df)
    
    logger.info(f"Fresher Filter: {len(kept_df)}/{initial_count} jobs are fresher-friendly ({rejected} rejected)")
    
    if rejected > 0:
        rejected_examples = df[~df['fresher_friendly']].head(3)
        for _, r in rejected_examples.iterrows():
            logger.debug(f"Rejected (Fresher): {r['title']} at {r['company']} (Desc Match: {extract_experience_requirement(str(r['description']))['raw_match']})")

    return kept_df

def apply_india_fresher_filters(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Main orchestrator for India and Fresher filtering.
    """
    if df.empty: return df
    
    input_count = len(df)
    
    # Read config
    target_country = config.get("target_country", "India")
    exp_config = config.get("experience", {})
    max_years = exp_config.get("max_years", 1)
    level = exp_config.get("level", "fresher")

    # 1. Extract experience details for all jobs (for metadata)
    df['experience_details'] = df['description'].apply(extract_experience_requirement)

    # 2. Apply India Filter
    if target_country and target_country.lower() != "any":
        df = filter_india_eligible(df)
        
    # 3. Apply Fresher Filter
    if level and level.lower() != "any":
        df = filter_fresher_friendly(df)

    # 4. Sort Remaining
    if not df.empty:
        # Sort fresher-friendly first, then by date (if available)
        sort_cols = ['fresher_friendly']
        ascending = [False]
        
        if 'date_posted' in df.columns:
            df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')
            sort_cols.append('date_posted')
            ascending.append(False)
            
        df = df.sort_values(by=sort_cols, ascending=ascending)

    removed = input_count - len(df)
    logger.info(f"India+Fresher Filter: {input_count} → {len(df)} jobs ({removed} removed)")
    
    return df

if __name__ == "__main__":
    import sys
    import os
    # Add parent directory to path to import logger_setup
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    from modules.logger_setup import setup_logging
    setup_logging()
    
    # Test with sample data covering all edge cases
    test_data = pd.DataFrame([
        {"title": "Junior ML Engineer", "company": "TCS", "location": "Remote - Worldwide", "description": "Entry level Machine Learning role. 0-1 years experience. Python, TensorFlow, PyTorch. Open to candidates globally."},
        {"title": "Senior AI Research Scientist", "company": "OpenAI", "location": "US Only", "description": "5+ years of experience in deep learning research required. Must be based in the United States. PhD preferred."},
        {"title": "Data Scientist", "company": "Flipkart", "location": "Remote", "description": "Looking for a Data Scientist with Python, SQL, and machine learning skills. Experience with pandas and scikit-learn preferred."},
        {"title": "AI/ML Intern", "company": "Infosys", "location": "India - Remote", "description": "Fresh graduates welcome! Work on NLP and computer vision projects. No prior industry experience needed. Training provided."},
        {"title": "Lead MLOps Architect", "company": "Amazon", "location": "Remote - APAC", "description": "Minimum 8 years of experience in ML infrastructure. Principal-level role for seasoned professionals. Kubernetes, Docker, AWS SageMaker."},
    ])
    
    test_config = {
        "target_country": "India",
        "experience": {"level": "fresher", "max_years": 1}
    }
    
    result = apply_india_fresher_filters(test_data, test_config)
    print(f"\nKept {len(result)}/{len(test_data)} jobs")
    print(result[['title', 'india_eligible', 'fresher_friendly']])
