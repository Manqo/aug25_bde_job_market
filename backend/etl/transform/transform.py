import glob
import json
import os
from typing import List

import pandas as pd


def load_json_to_df(file: str) -> pd.DataFrame:
    """Load a JSON file into a normalized pandas DataFrame."""
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.json_normalize(data)
    return df


def add_level_name(df: pd.DataFrame) -> pd.DataFrame:
    """Extract the first level name into a flat column."""
    df["levels.name"] = df["levels"].apply(lambda x: x[0]["name"])
    return df


def flatten_json(
    files_dir: str, cols: List[str], new_col_names: List[str], data_type: str
) -> pd.DataFrame:
    """
    Load and flatten multiple JSON files into a single DataFrame.
    Extracts selected fields, optionally enriches job data and concatenates all records into one DataFrame.

    Args:
        files_dir (str): Directory containing JSON files.
        cols (list[str]): Input columns to extract.
        new_col_names (list[str]): Output column names.
        data_type (str): Dataset type ("jobs", "companies", "salaries").
    Returns:
        pd.DataFrame: Combined DataFrame from all JSON files.
    """

    path = os.path.join(files_dir, "*")
    files = glob.glob(path)

    dfs = []

    for file in files:
        df = load_json_to_df(file)
        if data_type == "jobs":
            df = add_level_name(df)
        # Select and rename columns in the DataFrame
        df = df[cols]
        df.columns = new_col_names
        # add df to list of dataframes
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)
    return df_final
