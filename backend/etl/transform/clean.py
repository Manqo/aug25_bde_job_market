import logging
import os
from typing import List

import pandas as pd
from etl.transform.clean_helpers import (
    clean_location_adzuna,
    clean_location_muse,
    clean_string,
    drop_invalid_rows,
    extract_category,
    extract_level,
    log_null_values,
    remove_duplicates,
)

# ---------- Logging Setup ----------

logger = None


def setup_logging():
    global logger
    LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(LOG_DIR, exist_ok=True)

    LOG_FILE = os.path.join(LOG_DIR, "data_cleaning.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=LOG_FILE,
        filemode="a",
    )
    logger = logging.getLogger("elt_transform")
    return logger


# ---------- Type-specific and generic Cleaning Functions ----------


def clean_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize raw job data. Converts publication dates, enforces id types,
    removes duplicates, trims job names, normalizes locations, and applies common cleaning rules.
    Args:
        df (pd.DataFrame): Raw jobs DataFrame.
    Returns:
        pd.DataFrame: Cleaned jobs DataFrame.
    """

    df["publication_date"] = pd.to_datetime(
        df["publication_date"], utc=True, errors="coerce"
    )
    df = df.astype({"job_id": "int64", "company_id": "int64"})
    df = remove_duplicates(df, ["job_id"], "jobs")
    df["job_name"] = df["job_name"].str.strip()
    df["categories"] = df["categories"].apply(
        lambda x: x[0]["name"]
        if isinstance(x, list) and len(x) > 0 and "name" in x[0]
        else None
    )
    df["locations"] = df["locations"].apply(clean_location_muse)
    df = common_cleaning(df, "jobs", subset_delete_nulls=["locations"])
    return df


def clean_companies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize raw company data. Converts publication dates, enforces ID types,
    cleans descriptions, removes duplicates, normalizes locations, and applies common cleaning rules.
    Args:
        df (pd.DataFrame): Raw companies DataFrame.
    Returns:
        pd.DataFrame: Cleaned companies DataFrame.
    """

    df["publication_date"] = pd.to_datetime(
        df["publication_date"], utc=True, errors="coerce"
    )
    df = df.astype({"company_id": "int64"})
    df["description"] = (
        df["description"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    )
    df = remove_duplicates(df, ["company_id"], "companies")
    df["locations"] = df["locations"].apply(clean_location_muse)
    df = common_cleaning(df, "companies", subset_delete_nulls=[])
    return df


def clean_salaries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize raw salary data. Converts publication dates and salary fields, enforces
    ID types, removes duplicates, normalizes locations, and applies common cleaning rules.
    Args:
        df (pd.DataFrame): Raw salaries DataFrame.
    Returns:
        pd.DataFrame: Cleaned salaries DataFrame.
    """

    df["publication_date"] = pd.to_datetime(
        df["publication_date"], utc=True, errors="coerce"
    )
    df = df.astype({"adz_job_id": "int64"})
    df["salary_min"] = pd.to_numeric(df["salary_min"], errors="coerce")
    df["salary_max"] = pd.to_numeric(df["salary_max"], errors="coerce")
    df = remove_duplicates(df, ["adz_job_id"], "salaries")

    # extract and normalize titles and levels
    df["clean_title"] = df["adz_job_name"].apply(clean_string)
    df["categories"] = df["clean_title"].apply(extract_category)
    df["level"] = df["clean_title"].apply(extract_level)
    df = df.drop("clean_title", axis=1)

    # normalize locations
    df["locations"] = df["locations"].apply(clean_location_adzuna)

    df = common_cleaning(
        df, "salaries", subset_delete_nulls=["salary_min", "salary_max", "company_name"]
    )
    return df


def common_cleaning(
    df: pd.DataFrame, data_type: str, subset_delete_nulls: List[str]
) -> pd.DataFrame:
    """
    Apply shared final cleaning steps to a DataFrame. Replaces empty strings and empty lists/dicts with null values,
    logs missing data, and removes invalid rows.
    Args:
        df (pd.DataFrame): Input DataFrame.
        data_type (str): Dataset name for logging (e.g. "jobs", "companies", "salaries").
        subset_delete_nulls (list[str]): Columns required to be non-null.
    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """

    # transform empty strings and lists to null values
    df = df.replace(r"^$", pd.NA, regex=True)
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: pd.NA if isinstance(x, (list, dict)) and len(x) == 0 else x
        )
    # log and clean null values
    log_null_values(df, data_type)
    df = drop_invalid_rows(df, data_type, subset_delete_nulls)
    return df


# ---------- Main Cleaning Function ----------


def data_cleaning(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    Dispatch cleaning logic based on dataset type.
    Args:
        df (pd.DataFrame): Raw input DataFrame.
        data_type (str): Dataset type ("jobs", "companies", "salaries").
    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """

    logger.info(
        f"Starting data cleaning for type '{data_type}' with {len(df)} records."
    )

    if data_type == "jobs":
        cleaned_df = clean_jobs(df)
    elif data_type == "companies":
        cleaned_df = clean_companies(df)
    elif data_type == "salaries":
        cleaned_df = clean_salaries(df)
    else:
        raise ValueError(f"Unknown data type: {data_type}")

    logger.info(
        f"Finished cleaning '{data_type}'. Final record count: {len(cleaned_df)}"
    )
    return cleaned_df
