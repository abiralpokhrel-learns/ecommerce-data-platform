"""Load raw CSVs from data/raw/ into Postgres raw schema."""
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from loguru import logger

from db import get_engine
from config import INGESTION_CONFIG

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

# Configure logger
logger.remove()
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")


def load_csv_to_table(csv_path: Path, table_name: str, engine) -> int:
    """Load a single CSV into a Postgres table. Returns row count loaded."""
    schema, table = table_name.split(".")
    
    logger.info(f"Reading {csv_path.name}...")
    df = pd.read_csv(csv_path, low_memory=False)
    initial_rows = len(df)
    
    # Add audit columns
    df["_loaded_at"] = datetime.utcnow()
    df["_source_file"] = csv_path.name
    
    logger.info(f"Loading {initial_rows:,} rows into {table_name}...")
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="replace",  # For idempotency in dev. Switch to "append" with dedupe later.
        index=False,
        chunksize=10000,
        method="multi",
    )
    
    logger.success(f"Loaded {initial_rows:,} rows into {table_name}")
    return initial_rows


def verify_load(table_name: str, expected_rows: int, engine) -> bool:
    """Verify the row count matches expectation."""
    schema, table = table_name.split(".")
    query = f'SELECT COUNT(*) FROM {schema}."{table}"'
    actual_rows = pd.read_sql(query, engine).iloc[0, 0]
    
    if actual_rows == expected_rows:
        logger.success(f"Verified {table_name}: {actual_rows:,} rows")
        return True
    else:
        logger.error(f"MISMATCH {table_name}: expected {expected_rows:,}, got {actual_rows:,}")
        return False


def main():
    """Main entrypoint."""
    logger.info("=" * 60)
    logger.info("Starting raw ingestion")
    logger.info("=" * 60)
    
    engine = get_engine()
    
    # Verify connection
    # Verify connection
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.success("Database connection OK")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)
    
    results = {}
    failures = []
    
    for csv_filename, config in INGESTION_CONFIG.items():
        csv_path = RAW_DATA_DIR / csv_filename
        
        if not csv_path.exists():
            logger.error(f"File not found: {csv_path}")
            failures.append(csv_filename)
            continue
        
        try:
            rows_loaded = load_csv_to_table(csv_path, config["table"], engine)
            verified = verify_load(config["table"], rows_loaded, engine)
            results[config["table"]] = {"rows": rows_loaded, "verified": verified}
        except Exception as e:
            logger.error(f"Failed to load {csv_filename}: {e}")
            failures.append(csv_filename)
    
    # Summary
    logger.info("=" * 60)
    logger.info("Ingestion summary")
    logger.info("=" * 60)
    total_rows = sum(r["rows"] for r in results.values())
    logger.info(f"Tables loaded: {len(results)}")
    logger.info(f"Total rows: {total_rows:,}")
    
    if failures:
        logger.error(f"Failures: {failures}")
        sys.exit(1)
    
    logger.success("All tables loaded successfully")


if __name__ == "__main__":
    main()