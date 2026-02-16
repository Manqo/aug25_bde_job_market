from config.config import SUPABASE_DB, SUPABASE_SSL_MODE
from sqlalchemy import create_engine, text


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{SUPABASE_DB['user']}:{SUPABASE_DB['password']}@"
        f"{SUPABASE_DB['host']}:{SUPABASE_DB['port']}/{SUPABASE_DB['database']}?sslmode={SUPABASE_SSL_MODE}"
    )


def load_norm_tables():
    engine = get_engine()

    queries = [
        # INDUSTRIES
        """
        INSERT INTO norm.industries(name)
        SELECT DISTINCT industry_name as name
        FROM raw.companies
        WHERE industry_name IS NOT NULL
        AND TRIM(industry_name) <> ''
        AND TRIM(industry_name) <> 'None'
        ON CONFLICT (name) DO NOTHING;
        """,
        # CATEGORIES
        """
        INSERT INTO norm.categories(name)
        SELECT DISTINCT TRIM(categories) as name
        FROM raw.jobs
        WHERE categories IS NOT NULL
        AND TRIM(categories) <> ''
        AND TRIM(categories) <> 'None'
        ON CONFLICT (name) DO NOTHING;
        """,
        # LEVELS
        """
        INSERT INTO norm.levels(level)
        SELECT DISTINCT TRIM(level)
        FROM (
            SELECT level FROM raw.jobs
            UNION
            SELECT level FROM raw.salaries
        ) t
        WHERE level IS NOT NULL
        AND TRIM(level) <> ''
        AND TRIM(level) <> 'None'
        ON CONFLICT (level) DO NOTHING;
        """,
        # LOCATIONS
        """
        INSERT INTO norm.locations (city, subdivision_code, country_code)
        SELECT DISTINCT
            t.location_city AS city,
            t.location_state AS subdivision_code,
            t.location_country AS country_code
        FROM (
            SELECT location_city, location_state, location_country FROM raw.companies
            UNION ALL
            SELECT location_city, location_state, location_country FROM raw.jobs
            UNION ALL
            SELECT location_city, location_state, location_country FROM raw.salaries
        ) t
        WHERE t.location_city IS NOT NULL
        AND t.location_city <> ''
        AND NOT EXISTS (
                SELECT 1 
                FROM norm.locations nl
                WHERE nl.city = t.location_city
                AND nl.subdivision_code IS NOT DISTINCT FROM t.location_state
                AND nl.country_code    IS NOT DISTINCT FROM t.location_country
        );
        """,
        # COMPANIES
        """
        INSERT INTO norm.companies (id, description, name, publication_date, size)
        SELECT DISTINCT
            company_id,
            description,
            company_name,
            publication_date::timestamp,
            size
        FROM raw.companies
        ON CONFLICT (id) DO NOTHING;
        """,
        # COMPANIES_INDUSTRIES
        """
        INSERT INTO norm.companies_industries (company_id, industry_id)
        SELECT DISTINCT
            r.company_id,
            i.id as industry_id
        FROM (
            SELECT DISTINCT company_id, industry_name
            FROM raw.companies
        ) r
        JOIN norm.industries i 
            ON i.name = r.industry_name
        WHERE r.industry_name <> ''
        AND r.industry_name <> 'None'
        ON CONFLICT (company_id, industry_id) DO NOTHING;
        """,
        # COMPANIES_LOCATIONS
        """
        INSERT INTO norm.companies_locations (company_id, location_id)
        SELECT DISTINCT
            c.id as company_id,
            l.id as location_id
        FROM raw.companies r
        JOIN norm.companies c ON c.id = r.company_id
        JOIN norm.locations l
          ON l.city = r.location_city
         AND l.subdivision_code = r.location_state
         AND l.country_code  = r.location_country
        ON CONFLICT (company_id, location_id) DO NOTHING;
        """,
        # JOBS
        """
        INSERT INTO norm.jobs (company_id, name, level_id, category_id, publication_date)
        SELECT DISTINCT
            r.company_id,
            r.job_name as name,
            lvl.id,
            cat.id,
            r.publication_date::timestamp
        FROM raw.jobs r
        JOIN norm.companies c
          ON c.id = r.company_id           
        LEFT JOIN norm.levels lvl
          ON lvl.level = TRIM(r.level)
        LEFT JOIN norm.categories cat
          ON cat.name = r.categories
        WHERE lvl.id IS NOT NULL
        ON CONFLICT (company_id, name, publication_date) DO NOTHING;
        """,
        # JOBS_LOCATIONS
        """
        INSERT INTO norm.jobs_locations (job_id, location_id)
        SELECT DISTINCT
            j.id as job_id,
            l.id as location_id
        FROM raw.jobs r
        JOIN norm.jobs j
          ON j.company_id = r.company_id
         AND j.name = r.job_name
         AND j.publication_date = r.publication_date::timestamp
        JOIN norm.locations l
          ON l.city = r.location_city
         AND l.subdivision_code = r.location_state
         AND l.country_code  = r.location_country
        WHERE r.location_city IS NOT NULL
        AND r.location_city <> ''
        ON CONFLICT (job_id, location_id) DO NOTHING;
        """,
        # SALARIES
        """
        INSERT INTO norm.salaries (
            company_id, 
            location_id, 
            title,
            salary_min, 
            salary_max,
            level_id, 
            category_id
        )
        SELECT DISTINCT
            c.id,
            l.id,
            TRIM(s.adz_job_name),
            s.salary_min,
            s.salary_max,
            lvl.id,
            cat.id
        FROM raw.salaries s
        LEFT JOIN norm.companies c 
            ON LOWER(TRIM(c.name)) = LOWER(TRIM(s.company_name))
        LEFT JOIN norm.levels lvl 
            ON lvl.level = TRIM(s.level)
        LEFT JOIN norm.categories cat 
            ON cat.name = TRIM(s.categories)
        LEFT JOIN norm.locations l
          ON l.city = s.location_city
         AND l.subdivision_code = s.location_state
         AND l.country_code = s.location_country
        WHERE s.location_city IS NOT NULL
          AND s.location_city <> ''
          AND c.id IS NOT NULL
          AND l.id IS NOT NULL
        ON CONFLICT (company_id, location_id, title, level_id, category_id)
        DO NOTHING;
        """,
    ]

    with engine.begin() as conn:
        for q in queries:
            conn.execute(text(q))

    print("\n=== NORMALIZED TABLE COUNTS ===")
    counts = {
        "industries": "SELECT COUNT(*) FROM norm.industries;",
        "categories": "SELECT COUNT(*) FROM norm.categories;",
        "levels": "SELECT COUNT(*) FROM norm.levels;",
        "locations": "SELECT COUNT(*) FROM norm.locations;",
        "companies": "SELECT COUNT(*) FROM norm.companies;",
        "companies_industries": "SELECT COUNT(*) FROM norm.companies_industries;",
        "companies_locations": "SELECT COUNT(*) FROM norm.companies_locations;",
        "jobs": "SELECT COUNT(*) FROM norm.jobs;",
        "jobs_locations": "SELECT COUNT(*) FROM norm.jobs_locations;",
        "salaries": "SELECT COUNT(*) FROM norm.salaries;",
    }
    with engine.connect() as conn:
        for table, query in counts.items():
            print(f"{table}: {conn.execute(text(query)).scalar()} rows")
    print("================================\n")


if __name__ == "__main__":
    load_norm_tables()
