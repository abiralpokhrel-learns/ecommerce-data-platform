"""Upload raw CSVs from local data/raw/ to MinIO 'olist-raw' bucket."""
from pathlib import Path
from datetime import datetime
from loguru import logger

from storage import upload_file, RAW_BUCKET
from config import INGESTION_CONFIG

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def main():
    logger.info("Uploading raw CSVs to MinIO data lake")
    
    today = datetime.utcnow().strftime("%Y-%m-%d")
    
    for csv_filename in INGESTION_CONFIG.keys():
        local_path = RAW_DATA_DIR / csv_filename
        # Partition by date — common lake pattern
        object_key = f"olist/dt={today}/{csv_filename}"
        upload_file(local_path, RAW_BUCKET, object_key)
    
    logger.success(f"All files uploaded to s3://{RAW_BUCKET}/olist/dt={today}/")


if __name__ == "__main__":
    main()