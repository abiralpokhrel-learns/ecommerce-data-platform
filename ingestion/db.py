"""Database connection helper."""
import os
from contextlib import contextmanager
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "warehouse"),
    "user": os.getenv("POSTGRES_USER", "warehouse_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "warehouse_pass"),
}


def get_engine():
    """SQLAlchemy engine for pandas to_sql operations."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url)


@contextmanager
def get_connection():
    """Raw psycopg2 connection for fast COPY operations."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()