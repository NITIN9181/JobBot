import os
import json
import hashlib
import time
import logging
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from openai import OpenAI
from dotenv import load_dotenv
from modules.utils import retry

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

# Constants
CACHE_FILE = "output/score_cache.json"
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_MODEL = "nvidia/gpt-oss-120b"

# NVIDIA Models (change model name below as needed):
# - "nvidia/gpt-oss-120b"            ← RECOMMENDED, Best MoE reasoning
# - "meta/llama-3.1-8b-instruct"      ← Fast, good for basic scoring
# - "meta/llama-3.1-405b-instruct"    ← Extreme accuracy, high latency
# - "mistralai/mixtral-8x22b-instruct-v0.1" ← Powerful alternative
# - "google/gemma-2-27b-it"           ← Alternative

def create_user_profile(config: Dict[str, Any]) -> str:
    """
    Builds a text profile from configuration values to pass to the AI.
    """
    search_terms = ", ".join(config.get("search_terms", ["Not specified"]))
    skills = ", ".join(config.get("skills", ["Not specified"]))
    min_salary = config.get("min_salary", "Not specified")
    job_type = config.get("job_type", "Not specified")
    
    profile = f"""
I am looking for a remote job.
Target roles: {search_terms}
My skills: {skills}
Minimum salary expectation: {min_salary}
Preferred job type: {job_type}
"""
    # Add extra preferences if present
    if "preferences" in config:
        profile += f"Additional preferences: {config['preferences']}\n"
    
    return profile.strip()

def get_cache_key(job: Dict[str, Any]) -> str:
    """
    Generates an MD5 hash cache key from job title, company, and part of description.
    """
    title = str(job.get("title", ""))
    company = str(job.get("company", ""))
    description = str(job.get("description", ""))[:500]
    
    unique_str = f"{title}|{company}|{description}"
    return hashlib.md5(unique_str.encode("utf-8")).hexdigest()

def load_score_cache() -> Dict[str, Any]:
    """Loads the score cache from disk."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading cache: {e}")
        return {}

def save_score_cache(cache: Dict[str, Any]):
    """Saves the score cache to disk, cleaning expired entries."""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    
    # Clean expired entries (> 7 days)
    now = datetime.now()
    cleaned_cache = {}
    for key, data in cache.items():
        cached_date_str = data.get("cached_date")
        if cached_date_str:
            try:
                cached_date = datetime.strptime(cached_date_str, "%Y-%m-%d")
                if now - cached_date < timedelta(days=7):
                    cleaned_cache[key] = data
            except ValueError:
                cleaned_cache[key] = data # Keep if format is weird
        else:
            cleaned_cache[key] = data # Keep if no date
            
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cleaned_cache, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving cache: {e}")

def score_single_job(job: Dict[str, Any], user_profile: str, client: OpenAI, model: str) -> Dict[str, Any]:
    """
    Calls NVIDIA API to score a single job listing.
    """
    default_response = {"score": 50, "reason": "Could not analyze", "key_matches": [], "missing_skills": []}
    
    prompt = f"""Score how well this job matches the candidate's profile.

CANDIDATE PROFILE:
{user_profile}

JOB LISTING:
Title: {job.get('title', 'N/A')}
Company: {job.get('company', 'N/A')}
Description: {str(job.get('description', ''))[:1500]}

Respond in EXACTLY this JSON format and nothing else:
{{
  "score": <number from 0 to 100>,
  "reason": "<one sentence explaining why this score>",
  "key_matches": ["skill1", "skill2"],
  "missing_skills": ["skill3", "skill4"]
}}"""

    # model is now passed as an argument
    
    @retry(max_attempts=3, delay=5)
    def call_ai():
        return client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You are a job matching assistant. You ONLY respond in valid JSON format. No extra text, no markdown, no explanation outside the JSON."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.2,
            max_tokens=300,
            top_p=0.7
        )

    try:
        response = call_ai()
        
        content = response.choices[0].message.content.strip()
        
        # Clean JSON markdown if present
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]
        
        # Find the first { and last }
        start = content.find('{')
        end = content.rfind('}')
        if start != -1 and end != -1:
            content = content[start:end+1]
            
        data = json.loads(content)
        
        # Validation
        score = int(data.get("score", 50))
        reason = str(data.get("reason", "No reason provided"))
        key_matches = data.get("key_matches", [])
        missing_skills = data.get("missing_skills", [])
        
        if not isinstance(key_matches, list): key_matches = []
        if not isinstance(missing_skills, list): missing_skills = []
        
        return {
            "score": min(max(score, 0), 100),
            "reason": reason,
            "key_matches": key_matches,
            "missing_skills": missing_skills
        }

    except Exception as e:
        logger.error(f"Error scoring job {job.get('title')}: {e}")
        return default_response

def score_all_jobs(df: pd.DataFrame, config: Dict[str, Any]) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Analyzes a DataFrame of jobs and adds AI scores.
    Returns (scored_df, stats_dict)
    """
    stats = {
        "enabled": False,
        "total_scored": 0,
        "cached": 0,
        "new": 0,
        "top_score": 0,
        "top_job": "N/A",
        "avg_score": 0,
        "model": config.get("ai_scoring", {}).get("model", DEFAULT_MODEL)
    }
    
    if df.empty:
        return df, stats
        
    api_key = os.getenv("NVIDIA_API_KEY")
    if not api_key:
        logger.warning("NVIDIA_API_KEY not found in .env — skipping AI scoring")
        # Add empty columns to maintain consistency
        for col in ["ai_match_score", "ai_match_reason", "ai_key_matches", "ai_missing_skills"]:
            if col not in df.columns:
                df[col] = None
        return df, stats

    stats["enabled"] = True

    # Initialization
    client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=api_key)
    user_profile = create_user_profile(config)
    model = config.get("ai_scoring", {}).get("model", DEFAULT_MODEL)
    cache = load_score_cache()
    
    # Limit scoring to top N if too many
    max_jobs = config.get("ai_scoring", {}).get("max_jobs_to_score", 50)
    if len(df) > max_jobs:
        logger.info(f"Limiting AI scoring to top {max_jobs} jobs out of {len(df)}")
        if "skill_match_count" in df.columns:
            df = df.sort_values(by="skill_match_count", ascending=False)
        df = df.head(max_jobs).copy()
        
    results = []
    total = len(df)
    
    logger.info(f"Starting AI scoring for {total} jobs...")

    for i, (_, row) in enumerate(df.iterrows(), 1):
        job_dict = row.to_dict()
        cache_key = get_cache_key(job_dict)
        
        # Check cache
        if cache_key in cache:
            cached_data = cache[cache_key]
            logger.info(f"Using cached score for: {job_dict.get('title')} at {job_dict.get('company')}")
            score_data = {
                "ai_match_score": cached_data.get("score"),
                "ai_match_reason": cached_data.get("reason"),
                "ai_key_matches": cached_data.get("key_matches"),
                "ai_missing_skills": cached_data.get("missing_skills")
            }
            stats["cached"] += 1
        else:
            # Call API
            score_res = score_single_job(job_dict, user_profile, client, model)
            score_data = {
                "ai_match_score": score_res["score"],
                "ai_match_reason": score_res["reason"],
                "ai_key_matches": score_res["key_matches"],
                "ai_missing_skills": score_res["missing_skills"]
            }
            
            # Update cache
            cache[cache_key] = {
                "score": score_res["score"],
                "reason": score_res["reason"],
                "key_matches": score_res["key_matches"],
                "missing_skills": score_res["missing_skills"],
                "cached_date": datetime.now().strftime("%Y-%m-%d")
            }
            
            logger.info(f"Scoring job {i}/{total}: {job_dict.get('title')} — Score: {score_res['score']}%")
            stats["new"] += 1
            
            # Rate limiting
            if i < total:
                time.sleep(2)
        
        results.append(score_data)

    # Attach results to DataFrame
    final_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(results)], axis=1)
    
    # Sort by score
    final_df = final_df.sort_values(by="ai_match_score", ascending=False)
    
    threshold = config.get("ai_scoring", {}).get("min_score", 70)
    above_threshold_count = len(final_df[final_df["ai_match_score"] >= threshold])
    
    logger.info(f"AI Scoring complete. {above_threshold_count}/{total} jobs passed the {threshold}% threshold.")
    
    # Update stats for final results
    if not final_df.empty:
        stats["top_score"] = int(final_df["ai_match_score"].max())
        top_row = final_df.iloc[0]
        stats["top_job"] = f"{top_row['title']} at {top_row['company']}"
        stats["avg_score"] = int(final_df["ai_match_score"].mean())
        stats["above_threshold"] = len(final_df)
    else:
        stats["above_threshold"] = 0

    # Save cache
    save_score_cache(cache)
    
    return final_df, stats

def score_jobs_batch(df: pd.DataFrame, config: Dict[str, Any], batch_size: int = 5) -> tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Alternative batch scoring. 
    NOTE: Currently implemented as a sequential wrapper for reliability across different models.
    """
    logger.info(f"Using BATCH scoring mode for {len(df)} jobs (Batch size: {batch_size})")
    # For now, we reuse score_all_jobs as it handles caching and individual safety well.
    return score_all_jobs(df, config)

if __name__ == "__main__":
    # Test with sample jobs
    from modules.logger_setup import setup_logging
    setup_logging()
    
    sample_config = {
        "search_terms": ["Python Developer"],
        "skills": ["python", "django"],
        "min_salary": 60000,
        "job_type": "full-time",
        "ai_scoring": {"enabled": True, "min_score_threshold": 60}
    }

    sample_jobs = pd.DataFrame([
        {
            "title": "Senior Python Developer",
            "company": "TechCorp",
            "description": "We need a Python developer with Django and PostgreSQL experience.",
            "skill_match_count": 3
        }
    ])

    logger.info("Starting AI Scorer auto-verification...")
    
    try:
        scored, stats = score_all_jobs(sample_jobs, sample_config)
        logger.info(f"Scoring complete. Top score: {stats['top_score']}%")
    except Exception as e:
        logger.error(f"Verification failed: {e}")
