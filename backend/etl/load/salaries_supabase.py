import ast

import pandas as pd
from config.config import (
    PROCESSED_DATA_DIR,
    SALARIES_CSV_FILE,
    SUPABASE_DB,
    SUPABASE_SCHEMA,
    SUPABASE_SSL_MODE,
)
from sqlalchemy import create_engine


def load_salaries():
    csv_path = f"{PROCESSED_DATA_DIR}/{SALARIES_CSV_FILE}"

    engine = create_engine(
        f"postgresql+psycopg2://{SUPABASE_DB['user']}:{SUPABASE_DB['password']}@"
        f"{SUPABASE_DB['host']}:{SUPABASE_DB['port']}/{SUPABASE_DB['database']}?sslmode={SUPABASE_SSL_MODE}"
    )

    df = pd.read_csv(csv_path)

    def parse_list(value):
        if pd.isna(value) or str(value).strip() == "":
            return []
        try:
            return ast.literal_eval(value)
        except Exception:
            return []

    records = []

    for _, row in df.iterrows():
        loc_entries = parse_list(row.get("locations", "[]"))
        if not loc_entries:
            loc_entries = [None]

        for loc in loc_entries:
            if isinstance(loc, dict):
                city = loc.get("city")
                state = loc.get("subdivision_code") or loc.get("state")
                country = loc.get("country_code") or loc.get("country")
            else:
                city = state = country = None

            records.append(
                {
                    "adz_job_id": row.get("adz_job_id"),
                    "company_name": row.get("company_name"),
                    "adz_job_name": row.get("adz_job_name"),
                    "adz_category": row.get("adz_category"),
                    "publication_date": row.get("publication_date"),
                    "location_city": city,
                    "location_state": state,
                    "location_country": country,
                    "salary_min": row.get("salary_min"),
                    "salary_max": row.get("salary_max"),
                    "salary_is_predicted": row.get("salary_is_predicted"),
                    "categories": row.get("categories"),
                    "level": row.get("level"),
                }
            )

    expanded_df = pd.DataFrame(records)
    expanded_df.to_sql(
        "salaries", engine, schema=SUPABASE_SCHEMA, if_exists="append", index=False
    )

    print(f"✅ {len(expanded_df)} rows in {SUPABASE_SCHEMA}.salaries loaded.")
