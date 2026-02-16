import os

from dotenv import load_dotenv

# Repository root (two levels above backend/config)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Backend directory (one level above backend/config)
BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Load environment variables from 'env' file, if not running in Docker
if os.getenv("RUNNING_IN_DOCKER") != "1":
    env_file = os.path.join(REPO_ROOT, "deployment", ".env")
    if os.path.exists(env_file):
        load_dotenv(env_file)
    else:
        load_dotenv()

# Data directories (located at backend/data/*)
DATA_DIR = os.path.join(BACKEND_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")

RAW_DATA_JOBS_DIR = os.path.join(RAW_DATA_DIR, "jobs")
RAW_DATA_COMPANIES_DIR = os.path.join(RAW_DATA_DIR, "companies")
RAW_DATA_SALARIES_DIR = os.path.join(RAW_DATA_DIR, "salaries")

# Filenames produced by transform step
JOBS_CSV_FILE = "jobs.csv"
COMPANIES_CSV_FILE = "companies.csv"
SALARIES_CSV_FILE = "salaries.csv"

# Extraction input folder (inside the extract package)
EXTRACT_INPUT_DIR = os.path.join(BACKEND_DIR, "etl", "extract", "input")

# Adzuna API credentials (loaded from .env)
ADZUNA_APP_ID = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.environ.get("ADZUNA_APP_KEY", "")
ADZUNA_BASE_URL = "https://api.adzuna.com/v1/api/jobs"

# The Muse API base URLs
MUSE_BASE_URL_JOBS = "https://www.themuse.com/api/public/jobs"
MUSE_BASE_URL_COMPANIES = "https://www.themuse.com/api/public/companies"

# Supabase DB Config (for load step) - loaded from .env
SUPABASE_DB = {
    "user": os.environ.get("SUPABASE_USER", "postgres"),
    "password": os.environ.get("SUPABASE_PASSWORD", ""),
    "host": os.environ.get("SUPABASE_HOST", ""),
    "port": os.environ.get("SUPABASE_PORT", "5432"),
    "database": os.environ.get("SUPABASE_DATABASE", "postgres"),
}

SUPABASE_SCHEMA = os.environ.get("SUPABASE_SCHEMA", "raw")
SUPABASE_SSL_MODE = os.environ.get("SUPABASE_SSL_MODE", "require")

# ETL Token for securing API endpoints
ETL_TOKEN = os.getenv("ETL_TOKEN")
