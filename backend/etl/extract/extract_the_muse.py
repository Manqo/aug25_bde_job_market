import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import requests
from config.config import (
    MUSE_BASE_URL_COMPANIES,
    MUSE_BASE_URL_JOBS,
    RAW_DATA_COMPANIES_DIR,
    RAW_DATA_JOBS_DIR,
)

# ---------- CONFIGURATION ----------

BASE_URL_JOBS = MUSE_BASE_URL_JOBS
BASE_URL_COMPANIES = MUSE_BASE_URL_COMPANIES

OUTPUT_JOBS_PATH = os.path.join(RAW_DATA_JOBS_DIR, "muse_jobs_all.json")
OUTPUT_COMPANIES_PATH = os.path.join(RAW_DATA_COMPANIES_DIR, "muse_companies_all.json")

# Archive directory for old data
ARCHIVE_BASE_DIR = os.path.join(os.path.dirname(RAW_DATA_JOBS_DIR), "archive")

CATEGORIES = ["Computer and IT", "Data and Analytics", "Software Engineering"]

# Number of pages to fetch (API allows up to ~200)
TOTAL_PAGES = 200

# Delay between requests in seconds (10 min total ≈ 600 s / 400 requests ≈ 1.5 s)
DELAY = 1.5


# ---------- FUNCTION DEFINITIONS ----------


def fetch_paginated_data(base_url, params=None, total_pages=200):
    """Fetch up to total_pages pages slowly to avoid rate limits."""
    all_results = []
    for page in range(1, total_pages + 1):
        params = params or {}
        params["page"] = page
        try:
            response = requests.get(base_url, params=params, timeout=15)
            if response.status_code == 200:
                data = response.json()
                if not data.get("results"):
                    print(f"Page {page} returned no results. Stopping.")
                    break
                all_results.extend(data["results"])
                print(
                    f"Page {page} fetched: {len(data['results'])} items (total: {len(all_results)})"
                )
            else:
                print(
                    f"Error {response.status_code} on page {page}: {response.text[:100]}"
                )
                break
        except Exception as e:
            print(f"Exception on page {page}: {e}")
            break

        # Sleep to avoid rate limiting
        time.sleep(DELAY)

    return all_results


def archive_old_data(directory):
    """Move all files from directory to archive folder with timestamp."""
    if not os.path.exists(directory):
        return

    files = list(Path(directory).glob("muse_*_*.json"))
    if not files:
        return

    archive_dir = os.path.join(ARCHIVE_BASE_DIR, os.path.basename(directory))
    os.makedirs(archive_dir, exist_ok=True)

    archive_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_subfolder = os.path.join(archive_dir, f"archive_{archive_timestamp}")
    os.makedirs(archive_subfolder, exist_ok=True)

    for file_path in files:
        dest_path = os.path.join(archive_subfolder, file_path.name)
        shutil.move(str(file_path), dest_path)
        print(f"  📦 Archived: {file_path.name}")

    return archive_subfolder


def save_json(data, output_path):
    """Save combined JSON data to a file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output_path.replace(".json", f"_{timestamp}.json")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Saved {len(data)} records to {filename}")


# ---------- MAIN EXTRACTION LOGIC ----------


def main():
    print("\n🚀 Starting extraction from The Muse API...\n")

    # --- ARCHIVING OLD DATA ---
    print("📋 Archiving previous raw data...")
    archive_old_data(RAW_DATA_JOBS_DIR)
    archive_old_data(RAW_DATA_COMPANIES_DIR)
    print("✅ Archive complete\n")

    # --- JOBS ---
    all_jobs = []
    for category in CATEGORIES:
        print(f"\n📂 Fetching jobs for category: {category}")
        params = {"category": category}
        jobs = fetch_paginated_data(
            BASE_URL_JOBS, params=params, total_pages=TOTAL_PAGES
        )
        all_jobs.extend(jobs)
        print(f"✅ Category '{category}' completed: {len(jobs)} jobs\n")

    save_json(all_jobs, OUTPUT_JOBS_PATH)

    # --- COMPANIES ---
    print("\n🏢 Fetching companies (no filters)...")
    all_companies = fetch_paginated_data(BASE_URL_COMPANIES, total_pages=TOTAL_PAGES)
    save_json(all_companies, OUTPUT_COMPANIES_PATH)

    print("\n🎉 Extraction complete!")


# ---------- ENTRY POINT ----------
if __name__ == "__main__":
    main()
