import sys
import os
import json
import hashlib
import requests
from pathlib import Path
import logging
import time
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONITOR_STATE_PATH = Path("website_data/ministry_web_monitor.json")
MINISTRY_CODES = [59, 12, 39]
API_URL = "https://sansad.in/api_ls/question/qetFilteredQuestionsAns"

# Ensure directory exists
os.makedirs(MONITOR_STATE_PATH.parent, exist_ok=True)

def fetch_ministry_digest(ministry_code, session_number=5, loksabha_no=18):
    pdf_urls = []
    page_no = 1
    page_size = 20
    while True:
        params = {
            "loksabhaNo": str(loksabha_no),
            "sessionNumber": str(session_number),
            "pageNo": str(page_no),
            "locale": "en",
            "pageSize": str(page_size),
            "ministryCode": str(ministry_code)
        }
        r = requests.get(API_URL, params=params, timeout=40)
        if r.status_code != 200:
            break
        data = r.json()
        if not data or not data[0]["listOfQuestions"]:
            break
        questions = data[0]["listOfQuestions"]
        
        # Extract PDF URLs from questions
        for q in questions:
            if q.get("questionsFilePath"):
                pdf_urls.append(q["questionsFilePath"])
                
        page_no += 1
        time.sleep(0.2)
    
    # Remove duplicates and sort
    clean_set = sorted(set(pdf_urls))
    return clean_set

def calc_hash(pdf_list):
    m = hashlib.sha256()
    joined = "\n".join(sorted(pdf_list))
    m.update(joined.encode("utf-8"))
    return m.hexdigest()

def load_state():
    if MONITOR_STATE_PATH.exists():
        with open(MONITOR_STATE_PATH, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(MONITOR_STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)

def main():
    current_state = load_state()
    state_changed = False

    for code in MINISTRY_CODES:
        pdfs = fetch_ministry_digest(code)
        hash_val = calc_hash(pdfs)
        key = f"{code}"
        stored = current_state.get(key, {})
        if stored.get("hash") != hash_val:
            logger.info(f"Change detected for ministry code {code}")
            state_changed = True
            current_state[key] = {
                "hash": hash_val,
                "pdf_count": len(pdfs),
                "last_checked": time.strftime("%Y-%m-%dT%H:%M:%S")
            }
            logger.info(f"Triggering ingestion for ministry code {code}")
            subprocess.call([sys.executable, "sansad_client.py", str(code)])
        else:
            logger.info(f"No change for ministry code {code}, skipping ingestion.")
    save_state(current_state)

if __name__ == "__main__":
    main()
