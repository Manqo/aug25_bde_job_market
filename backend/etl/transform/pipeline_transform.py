import os

from config.config import (
    COMPANIES_CSV_FILE,
    JOBS_CSV_FILE,
    PROCESSED_DATA_DIR,
    RAW_DATA_COMPANIES_DIR,
    RAW_DATA_JOBS_DIR,
    RAW_DATA_SALARIES_DIR,
    SALARIES_CSV_FILE,
)
from etl.transform import clean, save, transform
from etl.transform.clean import setup_logging


def run_jobs():
    data_type = "jobs"
    cols = [
        "id",
        "company.id",
        "name",
        "levels.name",
        "publication_date",
        "locations",
        "categories",
    ]
    new_col_names = [
        "job_id",
        "company_id",
        "job_name",
        "level",
        "publication_date",
        "locations",
        "categories",
    ]
    df = transform.flatten_json(RAW_DATA_JOBS_DIR, cols, new_col_names, data_type)
    df = clean.data_cleaning(df, data_type)
    return df


def run_companies():
    data_type = "companies"
    cols = [
        "id",
        "name",
        "description",
        "publication_date",
        "size.name",
        "locations",
        "industries",
    ]
    new_col_names = [
        "company_id",
        "company_name",
        "description",
        "publication_date",
        "size",
        "locations",
        "industries",
    ]
    df = transform.flatten_json(RAW_DATA_COMPANIES_DIR, cols, new_col_names, data_type)
    df = clean.data_cleaning(df, data_type)
    return df


def run_salaries():
    data_type = "salaries"
    cols = [
        "id",
        "company.display_name",
        "title",
        "category.label",
        "created",
        "location.area",
        "salary_min",
        "salary_max",
        "salary_is_predicted",
    ]
    new_col_names = [
        "adz_job_id",
        "company_name",
        "adz_job_name",
        "adz_category",
        "publication_date",
        "locations",
        "salary_min",
        "salary_max",
        "salary_is_predicted",
    ]
    df = transform.flatten_json(RAW_DATA_SALARIES_DIR, cols, new_col_names, data_type)
    df = clean.data_cleaning(df, data_type)
    return df


def main():
    logger = setup_logging()
    logger.info("Starting transform pipeline...")
    # create folderpaths for storage of processed data if not exists
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

    # transform and save job data as csv
    df_jobs = run_jobs()
    save.save_as_csv(df_jobs, JOBS_CSV_FILE, PROCESSED_DATA_DIR)

    # transform and save company data as csv
    df_companies = run_companies()
    save.save_as_csv(df_companies, COMPANIES_CSV_FILE, PROCESSED_DATA_DIR)

    # transform and save company data as csv
    df_salaries = run_salaries()
    save.save_as_csv(df_salaries, SALARIES_CSV_FILE, PROCESSED_DATA_DIR)


if __name__ == "__main__":
    main()
