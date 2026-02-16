from config.config import SUPABASE_DB, SUPABASE_SSL_MODE
from sqlalchemy import create_engine, text


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{SUPABASE_DB['user']}:{SUPABASE_DB['password']}@"
        f"{SUPABASE_DB['host']}:{SUPABASE_DB['port']}/{SUPABASE_DB['database']}?sslmode={SUPABASE_SSL_MODE}"
    )


def truncate_raw_tables():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE raw.companies;"))
        conn.execute(text("TRUNCATE raw.jobs;"))
        conn.execute(text("TRUNCATE raw.salaries;"))
    print("🧹 raw tables truncated.")
