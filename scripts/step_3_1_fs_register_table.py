import os
from pathlib import Path
from scripts.utils import init_spark, get_logger
from datetime import timedelta, datetime
from feast import Entity, FeatureView, FeatureStore, Field
from feast.types import Int64, Float32
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from dotenv import load_dotenv

# Create a logger
log_file = f"/opt/airflow/logs/register_and_materialize_fts.log"
logger = get_logger(__name__, log_file)

# --- Environment Variable Loading ---
script_dir = "/opt/airflow/external_scripts/"  # Adjust this to the directory where your script is located
dotenv_path = os.path.join(script_dir, ".env")
if os.path.exists(dotenv_path):
    logger.info(f"Loading environment variables from: {dotenv_path}")
    load_dotenv(dotenv_path)
else:
    logger.error(".env file not found, relying on system environment variables.")

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
MINIO_ENDPOINT_LOCAL = os.getenv("S3_ENDPOINT_LOCAL")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

logger.info(f"Using MinIO Endpoint: {MINIO_ENDPOINT_LOCAL}")
logger.info(f"Using MinIO Bucket: {MINIO_BUCKET}")

# --- Data Path ---
DATA_PATH = f"s3a://{MINIO_BUCKET}/features/" 
logger.info(f"Expecting Delta table data at: {DATA_PATH}")

# --- Feast Repository Path ---
repo_path = "/opt/airflow/feature_store"

# Define the entity (e.g., customer) with a join key
customer = Entity(
    name="customer",
    join_keys=["user_id"],
    description="Customer Identifier"
)

transaction_source = SparkSource(
    name="transaction_source",
    path=DATA_PATH,
    timestamp_field="date", 
    file_format='delta'
)

# Define the FeatureView that ties the entity to the data source
customer_features = FeatureView(
    name="customer_features",
    entities=[customer],
    source=transaction_source,
    schema=[
       Field(name="num_transactions_l1w", dtype=Int64),
       Field(name="total_amount_l1w", dtype=Float32),
       Field(name="avg_amount_l1w", dtype=Float32),
       Field(name="min_amount_l1w", dtype=Float32),
       Field(name="max_amount_l1w", dtype=Float32),
       Field(name="num_vendors_l1w", dtype=Int64),
       Field(name="num_sources_l1w", dtype=Int64)
    ]
)

# --- Initialize Spark Session ---
spark = init_spark("FeastDeltaExample", local_mode=False)
fs = FeatureStore('/opt/airflow/feature_store')
logger.info("FeatureStore object created.")

logger.info("Applying feature definitions...")
fs.apply(customer)
fs.apply(customer_features)
logger.info(f"Feature registry updated at: {repo_path} / 'data' / 'registry.db'")

# --- Materialization ---
try:
    # Materialize data up to the current time
    fs.materialize_incremental(end_date=datetime.now())
    logger.info("Materialization complete.")
except Exception as e:
    logger.error(f"ERROR during materialization: {e}")
    raise e # Re-raise the exception

logger.info("\nFeature store setup process finished.")
logger.info(f"Online store configured at: {repo_path} / 'data' / 'online_store.db'")