import pandas as pd
import logging
from typing import List, Dict, Any, Optional

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Removes duplicate jobs based on title|company and job URL."""
    if df.empty: return df
    original_count = len(df)
    
    df_temp = df.copy()
    df_temp['title_norm'] = df_temp['title'].fillna('').str.lower().str.strip()
    df_temp['company_norm'] = df_temp['company'].fillna('').str.lower().str.strip()
    
    df = df.loc[df_temp.drop_duplicates(subset=['title_norm', 'company_norm']).index]
    if 'job_url' in df.columns:
        df = df.drop_duplicates(subset=['job_url'])
    
    removed = original_count - len(df)
    if removed > 0:
        logger.debug(f"Filter: Removed {removed} duplicate(s).")
    return df

def sort_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Sorts jobs by match quality and date."""
    if df.empty: return df
    sort_cols = []
    ascending = []
    
    if 'ai_match_score' in df.columns:
        sort_cols.append('ai_match_score')
        ascending.append(False)
    elif 'skill_match_count' in df.columns:
        sort_cols.append('skill_match_count')
        ascending.append(False)
        
    if 'date_posted' in df.columns:
        df['date_posted'] = pd.to_datetime(df['date_posted'], errors='coerce')
        sort_cols.append('date_posted')
        ascending.append(False)
        
    return df.sort_values(by=sort_cols, ascending=ascending) if sort_cols else df

def filter_jobs(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Applies criteria filters to raw job listings."""
    initial_count = len(df)
    if df.empty:
        logger.warning("Filter: Input empty. Skipping.")
        return df
        
    logger.info(f"Filter: Processing {initial_count} jobs.")

    # 1. Remote Only
    def is_remote(row):
        return (row.get('is_remote') is True) or ('remote' in str(row.get('location', '')).lower())
    df = df[df.apply(is_remote, axis=1)].copy()
    logger.debug(f"Filter (Remote): {len(df)} remaining.")

    # 2. Skills Match
    skills = [s.lower() for s in config.get('skills', [])]
    if skills:
        def get_matches(row):
            text = (str(row.get('title', '')) + " " + str(row.get('description', ''))).lower()
            return [s for s in skills if s in text]
        df['matched_skills'] = df.apply(get_matches, axis=1)
        df['skill_match_count'] = df['matched_skills'].apply(len)
        df = df[df['skill_match_count'] > 0].copy()
        logger.debug(f"Filter (Skills): {len(df)} remaining.")

    # 3. Salary
    min_sal = config.get('min_salary', 0)
    if min_sal > 0:
        def salary_ok(row):
            mi, ma = row.get('min_amount'), row.get('max_amount')
            if pd.isnull(mi) and pd.isnull(ma): return True
            return (pd.notnull(ma) and ma >= min_sal) or (pd.notnull(mi) and mi >= min_sal)
        df = df[df.apply(salary_ok, axis=1)].copy()
        logger.debug(f"Filter (Salary): {len(df)} remaining.")

    # 4. Job Type
    target_type = str(config.get('job_type', 'any')).lower().replace('-', '').strip()
    if target_type != 'any' and 'job_type' in df.columns:
        def type_ok(val):
            if pd.isnull(val) or not str(val).strip(): return True
            return str(val).lower().replace('-', '').strip() == target_type
        df = df[df['job_type'].apply(type_ok)].copy()
        logger.debug(f"Filter (Type): {len(df)} remaining.")

    # 5. Blacklist
    blacklist = [c.lower() for c in config.get('blacklisted_companies', [])]
    if blacklist and 'company' in df.columns:
        df = df[~df['company'].fillna('').str.lower().isin(blacklist)].copy()
        logger.debug(f"Filter (Blacklist): {len(df)} remaining.")

    df = remove_duplicates(df)
    df = sort_jobs(df)
    logger.info(f"Filter: Result {len(df)} matching jobs.")
    return df

if __name__ == "__main__":
    from modules.logger_setup import setup_logging
    setup_logging()
    logger.info("Filter engine test session.")
