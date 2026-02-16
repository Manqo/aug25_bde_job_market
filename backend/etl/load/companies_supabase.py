import ast
from itertools import product

import pandas as pd
from config.config import (
    COMPANIES_CSV_FILE,
    PROCESSED_DATA_DIR,
    SUPABASE_DB,
    SUPABASE_SCHEMA,
    SUPABASE_SSL_MODE,
)
from sqlalchemy import create_engine


def load_companies():
    csv_path = f"{PROCESSED_DATA_DIR}/{COMPANIES_CSV_FILE}"

    engine = create_engine(
        f"postgresql+psycopg2://{SUPABASE_DB['user']}:{SUPABASE_DB['password']}@"
        f"{SUPABASE_DB['host']}:{SUPABASE_DB['port']}/{SUPABASE_DB['database']}?sslmode={SUPABASE_SSL_MODE}"
    )

    df = pd.read_csv(csv_path)

    # Hilfsfunktion: parse JSON-ähnliche Listen
    def parse_list(value):
        if pd.isna(value) or str(value).strip() == "":
            return []
        try:
            return ast.literal_eval(value)
        except Exception:
            return []

    records = []

    for _, row in df.iterrows():
        # --- Locations extrahieren ---
        loc_entries = parse_list(row.get("locations", "[]"))
        if not loc_entries:
            loc_entries = [None]

        parsed_locations = []
        for loc in loc_entries:
            if isinstance(loc, dict):
                parsed_locations.append(
                    {
                        "location_city": loc.get("city"),
                        "location_state": loc.get("subdivision_code")
                        or loc.get("state"),
                        "location_country": loc.get("country_code")
                        or loc.get("country"),
                    }
                )
            else:
                parsed_locations.append(
                    {
                        "location_city": None,
                        "location_state": None,
                        "location_country": None,
                    }
                )

        # --- Industries extrahieren ---
        industry_entries = parse_list(row.get("industries", "[]"))
        if not industry_entries:
            industry_entries = [None]

        parsed_industries = []
        for ind in industry_entries:
            if isinstance(ind, dict):
                parsed_industries.append({"industry_name": ind.get("name")})
            else:
                parsed_industries.append({"industry_name": None})

        # --- Eine Zeile pro Kombination (Location × Industry) ---
        for loc, ind in product(parsed_locations, parsed_industries):
            records.append(
                {
                    "company_id": row.get("company_id"),
                    "company_name": row.get("company_name"),
                    "description": row.get("description"),
                    "publication_date": row.get("publication_date"),
                    "size": row.get("size"),
                    "location_city": loc["location_city"],
                    "location_state": loc["location_state"],
                    "location_country": loc["location_country"],
                    "industry_name": ind["industry_name"],
                }
            )

    expanded_df = pd.DataFrame(records)

    expanded_df.to_sql(
        "companies", engine, schema=SUPABASE_SCHEMA, if_exists="append", index=False
    )

    print(f"✅ {len(expanded_df)} rows in {SUPABASE_SCHEMA}.companies loaded.")
