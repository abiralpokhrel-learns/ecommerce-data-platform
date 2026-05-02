"""Daily ingestion DAG: local files -> MinIO -> Postgres raw."""
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook

from utils import INGESTION_PATH  # Side effect: adds to sys.path

# Now we can import our modules
from config import INGESTION_CONFIG
from storage import upload_file, get_s3_client, RAW_BUCKET
from load_raw import read_csv_from_s3
import pandas as pd


DEFAULT_ARGS = {
    "owner": "data_eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


@dag(
    dag_id="ingest_olist_daily",
    description="Daily ingestion: local CSVs -> MinIO -> Postgres",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "olist"],
)
def ingest_olist():

    @task
    def list_files_to_ingest() -> list[dict]:
        """Return a list of (filename, table) dicts for downstream tasks."""
        return [
            {"filename": fn, "table": cfg["table"]}
            for fn, cfg in INGESTION_CONFIG.items()
        ]

    @task
    def upload_to_minio(file_info: dict, **context) -> dict:
        """Upload one CSV to MinIO."""
        ds = context["ds"]  # 'YYYY-MM-DD' execution date
        local_path = Path("/opt/airflow/data/raw") / file_info["filename"]
        object_key = f"olist/dt={ds}/{file_info['filename']}"
        upload_file(local_path, RAW_BUCKET, object_key)
        return {**file_info, "s3_key": object_key}

    @task
    def load_to_postgres(file_info: dict) -> dict:
        """Read from MinIO and load to Postgres raw schema."""
        from datetime import datetime as dt
        
        df = read_csv_from_s3(RAW_BUCKET, file_info["s3_key"])
        df["_loaded_at"] = dt.utcnow()
        df["_source_file"] = file_info["s3_key"]
        
        schema, table = file_info["table"].split(".")
        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        engine = hook.get_sqlalchemy_engine()
        
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=10000,
            method="multi",
        )
        
        return {**file_info, "rows_loaded": len(df)}

    @task
    def summarize(results: list[dict]) -> None:
        """Print a summary of the run."""
        total = sum(r["rows_loaded"] for r in results)
        print(f"Loaded {len(results)} tables, {total:,} total rows")

    # Build the DAG with dynamic task mapping
    files = list_files_to_ingest()
    uploaded = upload_to_minio.expand(file_info=files)
    loaded = load_to_postgres.expand(file_info=uploaded)
    summarize(loaded)


ingest_olist()