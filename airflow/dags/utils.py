"""Helpers shared across DAGs."""
import sys

# Make ingestion/ importable inside the container
INGESTION_PATH = "/opt/airflow/ingestion"
if INGESTION_PATH not in sys.path:
    sys.path.insert(0, INGESTION_PATH)