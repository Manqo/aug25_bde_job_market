from config.config import SUPABASE_DB, SUPABASE_SSL_MODE
from sqlalchemy import create_engine, text


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{SUPABASE_DB['user']}:{SUPABASE_DB['password']}@"
        f"{SUPABASE_DB['host']}:{SUPABASE_DB['port']}/{SUPABASE_DB['database']}?sslmode={SUPABASE_SSL_MODE}"
    )


def load_star_tables():
    engine = get_engine()

    queries = [
        # =========================================
        # DIM COMPANIES
        # =========================================
        """
        INSERT INTO star.dim_companies (company_id, name, description, size, industry)
        SELECT DISTINCT
            c.id AS company_id,
            c.name,
            c.description,
            c.size,
            i.name AS industry
        FROM norm.companies c
        LEFT JOIN norm.companies_industries ci ON ci.company_id = c.id
        LEFT JOIN norm.industries i ON i.id = ci.industry_id
        WHERE c.id IS NOT NULL
        ON CONFLICT (company_id) DO NOTHING;
        """,
        # =========================================
        # DIM JOBS
        # =========================================
        """
        INSERT INTO star.dim_jobs (job_id, name)
        SELECT DISTINCT
            j.id AS job_id,
            j.name
        FROM norm.jobs j
        ON CONFLICT (job_id) DO NOTHING;
        """,
        # =========================================
        # DIM LEVELS
        # =========================================
        """
        INSERT INTO star.dim_levels (level_id, level)
        SELECT 
            l.id AS level_id,
            l.level
        FROM norm.levels l
        ON CONFLICT (level_id) DO NOTHING;
        """,
        # =========================================
        # DIM CATEGORIES
        # =========================================
        """
        INSERT INTO star.dim_categories (category_id, name)
        SELECT 
            c.id AS category_id,
            c.name
        FROM norm.categories c
        ON CONFLICT (category_id) DO NOTHING;
        """,
        # =========================================
        # DIM LOCATIONS
        # =========================================
        """
        INSERT INTO star.dim_locations (city, state, country)
        SELECT DISTINCT
            l.city,
            l.subdivision_code AS state,
            l.country_code     AS country
        FROM norm.locations l
        WHERE l.city IS NOT NULL
        AND NOT EXISTS (
                SELECT 1
                FROM star.dim_locations dl
                WHERE dl.city    = l.city
                AND dl.state   IS NOT DISTINCT FROM l.subdivision_code
                AND dl.country IS NOT DISTINCT FROM l.country_code
        );
        """,
        # =========================================
        # DIM DATE
        # =========================================
        """
        INSERT INTO star.dim_date (full_date, day, month, year)
        SELECT DISTINCT
            DATE(j.publication_date) AS full_date,
            EXTRACT(DAY FROM j.publication_date) AS day,
            EXTRACT(MONTH FROM j.publication_date) AS month,
            EXTRACT(YEAR FROM j.publication_date) AS year
        FROM norm.jobs j
        WHERE j.publication_date IS NOT NULL
        ON CONFLICT (full_date) DO NOTHING;
        """,
        # =========================================
        # FACT JOB POSTINGS
        # =========================================
        """
        INSERT INTO star.fact_job_postings (
            job_key, 
            company_key, 
            location_key, 
            date_key, 
            category_key, 
            level_key,
            salary_min, 
            salary_max
        )
        SELECT 
            dj.job_key,
            dc.company_key,
            dl.location_key,
            dd.date_key,
            dcat.category_key,
            dlev.level_key,
            s.salary_min,
            s.salary_max
        FROM norm.jobs j
        JOIN star.dim_jobs dj        
            ON dj.job_id       = j.id
        JOIN star.dim_companies dc  
             ON dc.company_id   = j.company_id
        JOIN star.dim_levels dlev    
            ON dlev.level_id   = j.level_id
        JOIN star.dim_categories dcat 
            ON dcat.category_id = j.category_id
        JOIN star.dim_date dd        
            ON dd.full_date    = DATE(j.publication_date)

        LEFT JOIN norm.jobs_locations jl 
            ON jl.job_id = j.id
        LEFT JOIN norm.locations l      
             ON l.id      = jl.location_id

        LEFT JOIN star.dim_locations dl
               ON LOWER(TRIM(dl.city)) = LOWER(TRIM(l.city))
              AND LOWER(TRIM(dl.state)) = LOWER(TRIM(l.subdivision_code))
              AND LOWER(TRIM(dl.country)) = LOWER(TRIM(l.country_code))

        LEFT JOIN LATERAL (
            SELECT s1.*
            FROM norm.salaries s1
            WHERE s1.company_id  = j.company_id
              AND s1.location_id = l.id
              AND s1.level_id = j.level_id
              AND s1.category_id = j.category_id
            ORDER BY s1.id DESC
            LIMIT 1
        ) s ON TRUE

        WHERE NOT EXISTS (
            SELECT 1
            FROM star.fact_job_postings f
            WHERE f.job_key      = dj.job_key
              AND f.company_key  = dc.company_key
              AND f.date_key     = dd.date_key
              AND f.category_key = dcat.category_key
              AND f.level_key    = dlev.level_key
              AND f.location_key IS NOT DISTINCT FROM dl.location_key
        );
        """,
    ]

    # ----------------------------
    # Execute queries
    # ----------------------------
    with engine.begin() as conn:
        for q in queries:
            conn.execute(text(q))

    # ----------------------------
    # Print counts
    # ----------------------------
    count_queries = {
        "dim_companies": "SELECT COUNT(*) FROM star.dim_companies;",
        "dim_jobs": "SELECT COUNT(*) FROM star.dim_jobs;",
        "dim_levels": "SELECT COUNT(*) FROM star.dim_levels;",
        "dim_categories": "SELECT COUNT(*) FROM star.dim_categories;",
        "dim_locations": "SELECT COUNT(*) FROM star.dim_locations;",
        "dim_date": "SELECT COUNT(*) FROM star.dim_date;",
        "fact_job_postings": "SELECT COUNT(*) FROM star.fact_job_postings;",
    }

    print("\n=== STAR SCHEMA TABLE COUNTS ===")
    with engine.connect() as conn:
        for table, query in count_queries.items():
            print(f"{table}: {conn.execute(text(query)).scalar()} rows")
    print("================================\n")


if __name__ == "__main__":
    load_star_tables()
