from config.config import SUPABASE_DB, SUPABASE_SSL_MODE
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


def get_engine():

    SUPABASE_DB_URL = (
        f"postgresql+psycopg2://{SUPABASE_DB['user']}:{SUPABASE_DB['password']}@"
        f"{SUPABASE_DB['host']}:{SUPABASE_DB['port']}/{SUPABASE_DB['database']}?sslmode={SUPABASE_SSL_MODE}"
    )

    try:
        engine = create_engine(SUPABASE_DB_URL, pool_pre_ping=True)
        return engine

    except OperationalError as e:
        print(f"Database connection failed: {e}")
        raise
    except Exception as e:
        print(f"Engine creation failed: {e}")
        raise


if __name__ == "__main__":

    # Try connecting
    try:
        engine = get_engine()
        with engine.connect() as conn:
            print("Connected to database successfully!")
    except Exception as e:
        print("DATABASE ERROR:", e)
