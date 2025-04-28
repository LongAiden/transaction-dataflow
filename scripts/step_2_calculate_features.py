import sys
import os
from scripts.utils import init_spark, get_logger
import datetime as dt
import pandas as pd
import numpy as np
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from dotenv import load_dotenv
from minio import Minio

dotenv_path = os.path.join("/opt/airflow/external_scripts/", '.env')  # Assuming .env is in the same directory
load_dotenv(dotenv_path)

# Environment variables
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW")
DATA_PATH = f"s3a://{MINIO_BUCKET_RAW}/daily/"
FEATURES_PATH = f"s3a://{MINIO_BUCKET}/features/"
RUN_DATE_STR = sys.argv[1]

def main(RUN_DATE_STR=None):
    if RUN_DATE_STR is None:
        RUN_DATE_STR = RUN_DATE_STR
    
    RUN_DATE_STR = sys.argv[1]
    RUN_DATE_STR_7DAYS = (dt.datetime.strptime(RUN_DATE_STR, "%Y-%m-%d") - dt.timedelta(days=7)).strftime('%Y-%m-%d')

    log_file = f"/opt/airflow/logs/2_calculate_fts/{RUN_DATE_STR}.log"
    logger = get_logger(__name__, log_file)
    
    try:
        RUN_DATE = dt.datetime.strptime(RUN_DATE_STR, "%Y-%m-%d")
        RUN_DATE_STR_7DAYS = (RUN_DATE - dt.timedelta(days=7)).strftime('%Y-%m-%d')
    except ValueError:
        logger.error(f"Error: Invalid date format for RUN_DATE_STR. Expected YYYY-MM-DD, got '{RUN_DATE_STR}'")
        sys.exit(1)
        
    logger.info(f"Starting feature calculation from {RUN_DATE_STR_7DAYS} to {RUN_DATE_STR}")
    logger.info(f"MinIO Endpoint: {MINIO_ENDPOINT}, MinIO Bucket: {MINIO_BUCKET}")

    # Create Spark session configured for MinIO (S3A)
    spark = init_spark("FeastDeltaExample", local_mode=False)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
 
    transaction_data_daily = spark.read.format("delta").load(DATA_PATH)\
                                .where(f'''date between "{RUN_DATE_STR_7DAYS}" and 
                                                        "{RUN_DATE_STR}"''')
        

    # Read streaming data from Kafka topic "cdc_transaction"
    try:
        distinct_dates = transaction_data_daily.filter(F.col("date").isNotNull()).select("date").distinct().count()
        logger.info(f"Distinct non-null dates in the data: {distinct_dates}")
    except Exception as e:
        logger.error(f"Error counting distinct non-null dates: {e}", exc_info=True)
        # Add more specific error handling or re-raise if needed
        sys.exit(1)
    
    # Check if there are at least 7 distinct dates`
    if distinct_dates < 7:
        # Log the values using the logger
        logger.error(f"Error: Found only {distinct_dates} distinct dates in the data. Minimum required is 7 days.")
        sys.exit(1)
                
    # Calculate lxw features
    time_window = "l1w"
    agg_fts = transaction_data_daily.groupBy("User ID")\
                     .agg(F.count("Transaction ID").alias(f"num_transactions_{time_window}"),
                          F.sum("Amount").alias(f"total_amount_{time_window}"),
                          F.avg("Amount").alias(f"avg_amount_{time_window}"),
                          F.min("Amount").alias(f"min_amount_{time_window}"),
                          F.max("Amount").alias(f"max_amount_{time_window}"),
                          F.countDistinct("Vendor").alias(f"num_vendors_{time_window}"),
                          F.countDistinct("Sources").alias(f"num_sources_{time_window}"))\
                        .withColumnRenamed("User ID", "user_id")\
                        .withColumn("date", F.lit(RUN_DATE_STR))
    
    agg_fts.show(5)

    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,  # Your MinIO server endpoint
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Set to True if using HTTPS
    )

    # Create bucket if it doesn't exist
    bucket = MINIO_BUCKET
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    agg_fts.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("date") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(FEATURES_PATH)
        
    logger.info(f"Data written to MinIO bucket: {bucket}/features")
    logger.info("Feature calculation completed successfully.")

if __name__ == "__main__":
    main()