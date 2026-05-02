"""MinIO (S3-compatible) client wrapper."""
import os
from pathlib import Path
import boto3
from botocore.client import Config
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    "region_name": "us-east-1",  # MinIO doesn't care, but boto3 needs something
    "config": Config(signature_version="s3v4"),
}

RAW_BUCKET = "olist-raw"
PROCESSED_BUCKET = "olist-processed"


def get_s3_client():
    """Return a boto3 S3 client configured for MinIO."""
    return boto3.client("s3", **MINIO_CONFIG)


def upload_file(local_path: Path, bucket: str, object_key: str) -> None:
    """Upload a local file to a MinIO bucket."""
    client = get_s3_client()
    logger.info(f"Uploading {local_path.name} to s3://{bucket}/{object_key}")
    client.upload_file(str(local_path), bucket, object_key)
    logger.success(f"Uploaded {object_key}")


def download_file(bucket: str, object_key: str, local_path: Path) -> None:
    """Download a file from MinIO to local path."""
    client = get_s3_client()
    logger.info(f"Downloading s3://{bucket}/{object_key}")
    client.download_file(bucket, object_key, str(local_path))


def list_objects(bucket: str, prefix: str = "") -> list[str]:
    """List object keys in a bucket."""
    client = get_s3_client()
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]