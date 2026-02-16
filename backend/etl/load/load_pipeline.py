import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# load csv into raw
from etl.load.companies_supabase import load_companies
from etl.load.jobs_supabase import load_jobs

# load raw into norm
from etl.load.load_norm_tables import load_norm_tables

# load norm into star
from etl.load.load_star_tables import load_star_tables
from etl.load.salaries_supabase import load_salaries

# truncate raw tables
from etl.load.truncate_raw import truncate_raw_tables


def main():
    print("Start truncate raw tables...")
    truncate_raw_tables()
    print("Truncated raw tables...")

    print("Start loading CSVs into raw tables...")
    load_companies()
    load_jobs()
    load_salaries()
    print("CSV data loaded into raw tables.\n")

    print("Start loading normalized tables...")
    load_norm_tables()
    print("Normalized tables loaded.\n")

    print("Start loading star schema tables...")
    load_star_tables()
    print("Star schema tables loaded.\n")

    print("All data successfully loaded.")


if __name__ == "__main__":
    main()
