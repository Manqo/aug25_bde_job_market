import json
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from config.config import (
    ADZUNA_APP_ID,
    ADZUNA_APP_KEY,
    ADZUNA_BASE_URL,
    RAW_DATA_JOBS_DIR,
    RAW_DATA_SALARIES_DIR,
)

# ---------- CONFIGURATION ----------

# Adzuna credentials and base URL are read from config (allow env override)
APP_ID = ADZUNA_APP_ID
APP_KEY = ADZUNA_APP_KEY
BASE_URL = ADZUNA_BASE_URL

# Input & Output paths (use configured directories)
JOBS_INPUT_DIR = RAW_DATA_JOBS_DIR
OUTPUT_PATH = os.path.join(RAW_DATA_SALARIES_DIR, "adzuna_it_jobs_by_companies.json")

# Archive directory for old data
ARCHIVE_BASE_DIR = os.path.join(os.path.dirname(RAW_DATA_SALARIES_DIR), "archive")

# Supported countries
COUNTRIES = ["us"]

# Delay between API calls to avoid rate-limiting
DELAY = 2.0


# ---------- FUNCTIONS ----------


def find_latest_muse_jobs_file(directory):
    """Find the latest muse_jobs_all_*.json file in the directory."""
    if not os.path.exists(directory):
        raise FileNotFoundError(f"Jobs directory not found: {directory}")

    files = sorted(Path(directory).glob("muse_jobs_all_*.json"), reverse=True)
    if not files:
        raise FileNotFoundError(f"No muse_jobs_all_*.json files found in {directory}")

    latest_file = str(files[0])
    print(f"Found latest jobs file: {os.path.basename(latest_file)}")
    return latest_file


def load_companies_from_jobs(jobs_file_path):
    """
    Read company names from muse_jobs_all.json file using pandas.
    Extracts unique company names, removes duplicates, and returns sorted list.
    """
    if not os.path.exists(jobs_file_path):
        raise FileNotFoundError(f"Jobs file not found: {jobs_file_path}")

    with open(jobs_file_path, "r", encoding="utf-8") as f:
        jobs_data = json.load(f)

    # Convert to pandas DataFrame for easier manipulation
    df = pd.DataFrame(jobs_data)

    # Extract company names from nested 'company' column
    company_names = set()
    for company in df["company"]:
        if isinstance(company, dict) and "name" in company:
            name = company["name"].strip()
            if name:
                company_names.add(name)

    companies = sorted(list(company_names))
    print(f"Extracted {len(companies)} unique companies from jobs file")
    return companies


def fetch_adzuna_page(country, company):
    """Fetch one page of IT job results from Adzuna for a given country and company using 'it-jobs' category."""
    url = f"{BASE_URL}/{country}/search/1"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": 50,
        "company": company,
        "category": "it-jobs",
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            print(f"{country.upper()} | {company[:30]}... → {len(results)} results")
            return results
        else:
            print(f"Error {response.status_code} for {country.upper()} / {company}")
            return []
    except Exception as e:
        print(f"Exception for {country.upper()} / {company}: {e}")
        return []


def archive_old_data(directory):
    """Move all files from directory to archive folder with timestamp."""
    if not os.path.exists(directory):
        return

    files = list(Path(directory).glob("adzuna_*_*.json"))
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
    """Save results into a single JSON file with timestamp."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = output_path.replace(".json", f"_{timestamp}.json")

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(data)} records to {filename}")


# ---------- MAIN ----------


def main():
    print(
        "Starting Adzuna IT job extraction (by company from Muse jobs, category=it-jobs)..."
    )

    # --- ARCHIVING OLD DATA ---
    print("\n📋 Archiving previous salary data...")
    archive_old_data(RAW_DATA_SALARIES_DIR)
    print("✅ Archive complete\n")

    all_results = []

    # Find latest muse_jobs_all_*.json file
    latest_jobs_file = find_latest_muse_jobs_file(JOBS_INPUT_DIR)

    # Extract unique companies from jobs file
    companies = load_companies_from_jobs(latest_jobs_file)

    for country in COUNTRIES:
        for company in companies:
            results = fetch_adzuna_page(country, company)
            for job in results:
                job["_country"] = country
                job["_company_query"] = company
            all_results.extend(results)
            time.sleep(DELAY)

    save_json(all_results, OUTPUT_PATH)
    print("IT company-based extraction (category=it-jobs) complete.")


if __name__ == "__main__":
    main()
