import ast

import pandas as pd
from config.config import (
    JOBS_CSV_FILE,
    PROCESSED_DATA_DIR,
    SUPABASE_DB,
    SUPABASE_SCHEMA,
    SUPABASE_SSL_MODE,
)
from sqlalchemy import create_engine


def load_jobs():
    csv_path = f"{PROCESSED_DATA_DIR}/{JOBS_CSV_FILE}"

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
        # Locations extrahieren (Liste mit Dicts)
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
                    "job_id": row.get("job_id"),
                    "company_id": row.get("company_id"),
                    "job_name": row.get("job_name"),
                    "level": row.get("level"),
                    "publication_date": row.get("publication_date"),
                    "location_city": city,
                    "location_state": state,
                    "location_country": country,
                    "categories": row.get("categories"),
                }
            )

    expanded_df = pd.DataFrame(records)

    expanded_df.to_sql(
        "jobs", engine, schema=SUPABASE_SCHEMA, if_exists="append", index=False
    )

    print(f"✅ {len(expanded_df)} rows in {SUPABASE_SCHEMA}.jobs loaded.")
