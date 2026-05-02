"""Load raw CSVs from MinIO into Postgres raw schema."""
import sys
import io
from pathlib import Path
from datetime import datetime
import pandas as pd
from loguru import logger

from db import get_engine
from storage import get_s3_client, RAW_BUCKET
from config import INGESTION_CONFIG

logger.remove()
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")


def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Read a CSV from S3/MinIO into a pandas DataFrame."""
    client = get_s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()), low_memory=False)


def load_csv_to_table(s3_key: str, table_name: str, engine) -> int:
    """Load a CSV from MinIO into a Postgres table."""
    schema, table = table_name.split(".")
    
    logger.info(f"Reading s3://{RAW_BUCKET}/{s3_key}")
    df = read_csv_from_s3(RAW_BUCKET, s3_key)
    initial_rows = len(df)
    
    df["_loaded_at"] = datetime.utcnow()
    df["_source_file"] = s3_key
    
    logger.info(f"Loading {initial_rows:,} rows into {table_name}")
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        chunksize=10000,
        method="multi",
    )
    
    logger.success(f"Loaded {initial_rows:,} rows into {table_name}")
    return initial_rows


def main():
    logger.info("=" * 60)
    logger.info("Starting raw ingestion (MinIO -> Postgres)")
    logger.info("=" * 60)
    
    engine = get_engine()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    
    failures = []
    total_rows = 0
    
    for csv_filename, config in INGESTION_CONFIG.items():
        s3_key = f"olist/dt={today}/{csv_filename}"
        try:
            rows = load_csv_to_table(s3_key, config["table"], engine)
            total_rows += rows
        except Exception as e:
            logger.error(f"Failed to load {csv_filename}: {e}")
            failures.append(csv_filename)
    
    logger.info("=" * 60)
    logger.info(f"Total rows: {total_rows:,}")
    
    if failures:
        logger.error(f"Failures: {failures}")
        sys.exit(1)
    
    logger.success("All tables loaded successfully from MinIO")


if __name__ == "__main__":
    main()