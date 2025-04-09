from datetime import timedelta, datetime
from pathlib import Path
import os
from pyspark.sql import SparkSession
from feast import Entity, FeatureView, FeatureStore, Field
from feast.types import Int64, Float32
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from dotenv import load_dotenv

# --- Environment Variable Loading ---
script_dir = "./scripts/"  # Adjust this to the directory where your script is located
dotenv_path = os.path.join(script_dir, ".env")
if os.path.exists(dotenv_path):
    print(f"Loading environment variables from: {dotenv_path}")
    load_dotenv(dotenv_path)
else:
    print(".env file not found, relying on system environment variables.")

MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
MINIO_ENDPOINT_LOCAL = os.getenv("S3_ENDPOINT_LOCAL")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

print(f"Using MinIO Endpoint: {MINIO_ENDPOINT_LOCAL}")
print(f"Using MinIO Bucket: {MINIO_BUCKET}")

# --- Data Path ---
DATA_PATH = f"s3a://{MINIO_BUCKET}/features/" 
print(f"Expecting Delta table data at: {DATA_PATH}")

# --- Feast Repository Path ---
repo_path = Path(os.getcwd()).resolve().parent / "feature_store"

# Define the entity (e.g., customer) with a join key
customer = Entity(
    name="customer",
    join_keys=["user_id"],
    description="Customer identifier"
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
    ttl=timedelta(days=30),
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

spark = SparkSession.builder \
    .appName("FeastDeltaExample") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT_LOCAL) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

fs = FeatureStore('./feature_store')
print("FeatureStore object created.")

print("Applying feature definitions...")
fs.apply(customer)
fs.apply(customer_features)
print(f"Feature registry updated at: {repo_path} / 'data' / 'registry.db'")

# --- Materialization ---
try:
    # Materialize data up to the current time
    fs.materialize_incremental(end_date=datetime.now())
    print("Materialization complete.")
except Exception as e:
    print(f"ERROR during materialization: {e}")
    raise e # Re-raise the exception

print("\nFeature store setup process finished.")
print(f"Online store configured at: {repo_path} / 'data' / 'online_store.db'")