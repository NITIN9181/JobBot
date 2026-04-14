import os
import json
import hashlib
import time
import logging
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from openai import OpenAI
from dotenv import load_dotenv
from modules.utils import retry

# Constants
CACHE_FILE = "output/verify_cache.json"
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_MODEL = "openai/gpt-oss-120b"

# Use logger inherited from root configuration
logger = logging.getLogger(__name__)

def get_cache_key(job: Dict[str, Any]) -> str:
    """
    Generates an MD5 hash cache key from job title, company, and part of description.
    Consistent with scorer.py.
    """
    title = str(job.get("title", ""))
    company = str(job.get("company", ""))
    description = str(job.get("description", ""))[:500]
    
    unique_str = f"{title}|{company}|{description}"
    return hashlib.md5(unique_str.encode("utf-8")).hexdigest()

def load_verify_cache() -> Dict[str, Any]:
    """Loads the verification cache from disk."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading verification cache: {e}")
        return {}

def save_verify_cache(cache: Dict[str, Any]):
    """Saves the verification cache to disk, cleaning expired entries."""
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
                cleaned_cache[key] = data
        else:
            cleaned_cache[key] = data
            
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(cleaned_cache, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving verification cache: {e}")

def verify_single_job(job: Dict[str, Any], client: OpenAI, model: str) -> Dict[str, Any]:
    """
    Calls NVIDIA API to verify a single job listing.
    """
    default_response = {
        "is_legitimate": True, 
        "india_eligible": True, 
        "fresher_friendly": True, 
        "confidence": 30, 
        "red_flags": ["verification_failed"], 
        "legitimacy_reason": "Could not verify", 
        "india_reason": "Could not verify",
        "fresher_reason": "Could not verify", 
        "estimated_experience_years": 0,
        "company_type": "unknown"
    }
    
    prompt = f"""You are a job listing verification expert. Analyze this job listing carefully and determine THREE things.

JOB LISTING:
Title: {job.get('title', 'N/A')}
Company: {job.get('company', 'N/A')}
Location: {job.get('location', 'Not specified')}
Source: {job.get('source_platform', 'Unknown')}
Description: {str(job.get('description', ''))[:2000]}

Analyze and respond in EXACTLY this JSON format:
{{
  "is_legitimate": true/false,
  "legitimacy_reason": "<one sentence explaining why this is or isn't a real job>",
  "india_eligible": true/false,
  "india_reason": "<one sentence explaining why someone in India can or cannot apply>",
  "fresher_friendly": true/false,
  "fresher_reason": "<one sentence about experience requirements>",
  "estimated_experience_years": <number or 0 if entry level>,
  "confidence": <0-100>,
  "red_flags": ["flag1", "flag2"],
  "company_type": "<startup/enterprise/agency/unknown>"
}}

VERIFICATION RULES:
- is_legitimate: FALSE if description is vague, company seems fake, promises unrealistic pay for no work, or is a known scam pattern (MLM, "work from home stuffing envelopes", crypto schemes)
- india_eligible: FALSE if listing explicitly requires US/EU/UK residency, citizenship, security clearance, or specifies timezone that excludes IST (e.g., "must work PST hours" is borderline but OK, "must be in US office" is FALSE)
- fresher_friendly: FALSE if the role explicitly requires 3+ years of professional experience. TRUE if it says entry-level, junior, 0-2 years, or doesn't mention experience requirements
- confidence: How confident you are in your assessment (100 = very certain, 50 = guessing)
- red_flags: List specific concerns like ["vague description", "no company website", "unrealistic salary"]"""

    @retry(max_attempts=2, delay=5)
    def call_ai():
        return client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system", 
                    "content": "You are a job listing verification assistant. You ONLY respond in valid JSON format. Be strict about legitimacy — when in doubt, flag it."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.1,
            max_tokens=500
        )

    try:
        response = call_ai()
        content = response.choices[0].message.content.strip()
        
        # Clean JSON markdown if present
        if content.startswith("```json"):
            content = content[7:]
        if content.endswith("```"):
            content = content[:-3]
        
        # Extract valid JSON object
        start = content.find('{')
        end = content.rfind('}')
        if start != -1 and end != -1:
            content = content[start:end+1]
            
        data = json.loads(content)
        
        # Basic validation and type forcing
        return {
            "is_legitimate": bool(data.get("is_legitimate", True)),
            "legitimacy_reason": str(data.get("legitimacy_reason", "N/A")),
            "india_eligible": bool(data.get("india_eligible", True)),
            "india_reason": str(data.get("india_reason", "N/A")),
            "fresher_friendly": bool(data.get("fresher_friendly", True)),
            "fresher_reason": str(data.get("fresher_reason", "N/A")),
            "estimated_experience_years": data.get("estimated_experience_years", 0),
            "confidence": min(max(int(data.get("confidence", 50)), 0), 100),
            "red_flags": data.get("red_flags", []),
            "company_type": str(data.get("company_type", "unknown"))
        }

    except Exception as e:
        logger.error(f"Error verifying job {job.get('title')}: {e}")
        return default_response

def verify_all_jobs(df: pd.DataFrame, config: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Analyzes a DataFrame of jobs and adds AI verification results.
    Returns (filtered_df, stats_dict)
    """
    stats = {
        "total_verified": 0,
        "legitimate": 0,
        "suspicious": 0,
        "rejected": 0,
        "india_eligible": 0, 
        "fresher_friendly": 0,
        "cached": 0,
        "avg_confidence": 0,
        "enabled": False
    }
    
    if df.empty:
        return df, stats
        
    api_key = os.getenv("NVIDIA_API_KEY")
    if not api_key:
        logger.warning("NVIDIA_API_KEY not found — skipping AI verification")
        # Add default columns for consistency
        df["verified"] = True
        df["verification_confidence"] = 0
        df["verification_red_flags"] = [[] for _ in range(len(df))]
        df["legitimacy_status"] = "unverified"
        df["india_verified"] = True
        df["fresher_verified"] = True
        df["verification_notes"] = "API Key missing"
        return df, stats

    stats["enabled"] = True
    client = OpenAI(base_url=NVIDIA_BASE_URL, api_key=api_key)
    model = config.get("ai_scoring", {}).get("model", DEFAULT_MODEL)
    cache = load_verify_cache()
    
    # Config limits
    max_jobs = config.get("verification", {}).get("max_jobs_to_verify", 100)
    min_confidence = config.get("verification", {}).get("min_confidence", 70)
    
    if len(df) > max_jobs:
        logger.info(f"Limiting AI verification to top {max_jobs} jobs")
        df = df.head(max_jobs).copy()
        
    results = []
    total = len(df)
    confidences = []
    
    logger.info(f"Starting AI verification for {total} jobs...")

    for i, (_, row) in enumerate(df.iterrows(), 1):
        job_dict = row.to_dict()
        cache_key = get_cache_key(job_dict)
        
        # Skip India check if Himalayas (already pre-filtered by source)
        is_himalayas = job_dict.get("source_platform", "").lower() == "himalayas"
        
        if cache_key in cache:
            v_res = cache[cache_key]
            stats["cached"] += 1
            if is_himalayas:
                v_res["india_eligible"] = True # override
        else:
            v_res = verify_single_job(job_dict, client, model)
            if is_himalayas:
                v_res["india_eligible"] = True
                
            # Update cache
            v_res["cached_date"] = datetime.now().strftime("%Y-%m-%d")
            cache[cache_key] = v_res
            
            logger.info(f"Verified {i}/{total}: {job_dict.get('title')} ({v_res['confidence']}%)")
            
            # Rate limiting delay
            if i < total:
                time.sleep(1.5)
        
        # Process verification result logic
        is_legit = v_res.get("is_legitimate", True)
        is_india = v_res.get("india_eligible", True)
        is_fresher = v_res.get("fresher_friendly", True)
        conf = v_res.get("confidence", 50)
        
        verified = is_legit and is_india and is_fresher
        
        # Stats updates
        stats["total_verified"] += 1
        if is_legit: stats["legitimate"] += 1
        else: stats["suspicious"] += 1
        if is_india: stats["india_eligible"] += 1
        if is_fresher: stats["fresher_friendly"] += 1
        confidences.append(conf)
        
        status_label = "legitimate" if is_legit else "suspicious"
        
        # Construct summary notes
        notes = f"L: {v_res.get('legitimacy_reason')} | I: {v_res.get('india_reason')} | F: {v_res.get('fresher_reason')}"
        
        row_res = {
            "verified": verified,
            "verification_confidence": conf,
            "verification_red_flags": v_res.get("red_flags", []),
            "legitimacy_status": status_label,
            "india_verified": is_india,
            "fresher_verified": is_fresher,
            "verification_notes": notes
        }
        results.append(row_res)

    # Combine results with original DF
    verified_df = pd.concat([df.reset_index(drop=True), pd.DataFrame(results)], axis=1)
    
    # Filter based on AI confidence
    # Remove jobs where verified == False AND confidence >= threshold
    initial_count = len(verified_df)
    filtered_df = verified_df[
        (verified_df["verified"] == True) | 
        (verified_df["verification_confidence"] < min_confidence)
    ].copy()
    
    stats["rejected"] = initial_count - len(filtered_df)
    if confidences:
        stats["avg_confidence"] = int(sum(confidences) / len(confidences))
        
    save_verify_cache(cache)
    logger.info(f"Verification complete. Kept {len(filtered_df)}/{initial_count} jobs. {stats['rejected']} rejected.")
    
    return filtered_df, stats

def get_verification_summary(stats: Dict[str, Any]) -> str:
    """Returns a formatted summary string for terminal display."""
    if not stats.get("enabled"):
        return "Verification: [Disabled or No API Key]"
        
    total = stats["total_verified"]
    if total == 0: return "Verification: No jobs to verify"
    
    legit_pct = int((stats["legitimate"] / total) * 100)
    india_pct = int((stats["india_eligible"] / total) * 100)
    fresher_pct = int((stats["fresher_friendly"] / total) * 100)
    
    summary = f"""Verification Results:
├── Verified Legitimate: {stats['legitimate']}/{total} ({legit_pct}%)
├── India Eligible:      {stats['india_eligible']}/{total} ({india_pct}%)
├── Fresher Friendly:    {stats['fresher_friendly']}/{total} ({fresher_pct}%)
├── Rejected (Low Conf): {stats['rejected']}
├── Avg Confidence:       {stats['avg_confidence']}%
└── Cache Hits:           {stats['cached']}"""
    return summary

if __name__ == "__main__":
    import sys
    import io
    # Force UTF-8 encoding for Windows terminal compatibility
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    from modules.logger_setup import setup_logging
    setup_logging()
    load_dotenv()

    sample_jobs = pd.DataFrame([
        {
            "title": "Junior ML Engineer",
            "company": "Wipro",
            "location": "Remote - India",
            "description": "Looking for fresh graduates to join our Machine Learning team. Work on NLP and computer vision projects using Python, TensorFlow, and PyTorch. No prior industry experience required. Training provided.",
            "source_platform": "Himalayas"
        },
        {
            "title": "AI Data Scientist - Work From Home",
            "company": "FakeAI Corp",
            "location": "Anywhere",
            "description": "Earn $5000/week doing AI work from home! No experience needed! Just label data and get paid! Start immediately! Visit our suspicious site to apply.",
            "source_platform": "RemoteOK"
        },
        {
            "title": "Senior Staff ML Research Scientist",
            "company": "Google DeepMind",
            "location": "US Only",
            "description": "10+ years of experience in machine learning research. PhD required. Must have US work authorization. Published papers in NeurIPS/ICML required.",
            "source_platform": "LinkedIn"
        }
    ])

    test_config = {
        "verification": {"enabled": True, "min_confidence": 70, "reject_red_flags": True},
        "ai_scoring": {"model": DEFAULT_MODEL}
    }

    try:
        verified_df, stats = verify_all_jobs(sample_jobs, test_config)
        print(f"\nPassed verification: {len(verified_df)}/{len(sample_jobs)}")
        print(get_verification_summary(stats))
        
        for _, row in verified_df.iterrows():
            print(f"\n{row['title']} at {row['company']}")
            print(f"  Verified: {row['verified']} (Confidence: {row['verification_confidence']}%)")
            print(f"  Notes: {row['verification_notes']}")
            print(f"  Red Flags: {row['verification_red_flags']}")
    except Exception as e:
        logger.error(f"Test failed: {e}")
