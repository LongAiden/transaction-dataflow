import sys
import os
from .utils import init_spark, get_logger
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
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
CDC_TRANSACTION_TOPIC = os.getenv("CDC_TRANSACTION_TOPIC")
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW")

RUN_DATE_STR = sys.argv[1]

def main(RUN_DATE_STR=None):
    if RUN_DATE_STR is None:
        RUN_DATE_STR = RUN_DATE_STR
    
    RUN_DATE_STR = sys.argv[1]
    RUN_DATE_STR_7DAYS = (dt.datetime.strptime(RUN_DATE_STR, "%Y-%m-%d") - dt.timedelta(days=7)).strftime('%Y-%m-%d')

    log_file = f"/opt/airflow/logs/1_get_transaction_data/{RUN_DATE_STR}.log"
    logger = get_logger(__name__, log_file)
    logger.info(f"Starting get transaction data from a Kafka topic at {RUN_DATE_STR}")
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}, CDC Transaction Topic: {CDC_TRANSACTION_TOPIC}")
    logger.info(f"MinIO Endpoint: {MINIO_ENDPOINT}, MinIO Bucket: {MINIO_BUCKET_RAW}")

    # Create Spark session configured for MinIO (S3A)
    spark = init_spark(f"get_transaction_data_{RUN_DATE_STR}", local_mode=False)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    cdc_events = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", CDC_TRANSACTION_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Define the nested schema for transaction data fields
    transaction_fields = [
        T.StructField("id", T.IntegerType(), False),
        T.StructField("User ID", T.StringType(), True),
        T.StructField("Transaction ID", T.StringType(), True),
        T.StructField("Amount", T.DoubleType(), True),
        T.StructField("Vendor", T.StringType(), True),
        T.StructField("Sources", T.StringType(), True),
        T.StructField("Time", T.StringType(), True)
    ]

    # Define the nested schema for source
    source_fields = [
        T.StructField("version", T.StringType(), False),
        T.StructField("connector", T.StringType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("ts_ms", T.LongType(), False),
        T.StructField("snapshot", T.StringType(), True),
        T.StructField("db", T.StringType(), False),
        T.StructField("sequence", T.StringType(), True),
        T.StructField("schema", T.StringType(), False),
        T.StructField("table", T.StringType(), False),
        T.StructField("txId", T.LongType(), True),
        T.StructField("lsn", T.LongType(), True),
        T.StructField("xmin", T.LongType(), True)
    ]

    # Define the schema for transaction block
    transaction_block_fields = [
        T.StructField("id", T.StringType(), False),
        T.StructField("total_order", T.LongType(), False),
        T.StructField("data_collection_order", T.LongType(), False)
    ]

    # Complete CDC schema
    cdc_schema = T.StructType([
        T.StructField("schema", T.StructType([
            T.StructField("type", T.StringType()),
            T.StructField("fields", T.ArrayType(T.StringType())),
            T.StructField("optional", T.BooleanType()),
            T.StructField("name", T.StringType()),
            T.StructField("version", T.IntegerType())
        ])),
        T.StructField("payload", T.StructType([
            T.StructField("before", T.StructType(transaction_fields), True),
            T.StructField("after", T.StructType(transaction_fields), True), 
            T.StructField("source", T.StructType(source_fields), False),
            T.StructField("op", T.StringType(), False),
            T.StructField("ts_ms", T.LongType(), True),
            T.StructField("transaction", T.StructType(transaction_block_fields), True)
        ]))
    ])

    # Parse the JSON data
    cdc_df = cdc_events \
                .selectExpr("CAST(value AS STRING) as json_str") \
                .withColumn("parsed_data", F.from_json(F.col("json_str"), cdc_schema)) \
                .select("parsed_data.payload.*")

    cdc_df.show(5)

    # To get just the transaction data after the change
    transaction_data_deleted = cdc_df.where('''op = 'd' ''').select('before.id')
    transaction_data_daily = cdc_df.where('''op in ('c','u','r')''').select(
        "op", 
        "after.id", 
        "after.`User ID`", 
        "after.`Transaction ID`", 
        "after.Amount", 
        "after.Vendor", 
        "after.Sources", 
        "after.Time",
        "source.ts_ms"
    )

    w = Window.partitionBy("id").orderBy(F.col("ts_ms").desc())
    transaction_data_daily = transaction_data_daily.withColumn("rank", F.row_number().over(w)) \
                            .filter(F.col("rank") == 1) \
                            .join(transaction_data_deleted, on=['id'], how="leftanti")\
                            .withColumn("timestamp", F.to_timestamp(F.col("Time"), "yyyy-MM-dd HH:mm:ss")) \
                            .withColumn("date", F.to_date(F.col("timestamp")))\
                            .where(f'''date in ("{RUN_DATE_STR}")''')\
                            .drop('timestamp', 'ts_ms','rank','id')

    transaction_data_daily.show(5)

    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,  # Your MinIO server endpoint
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Set to True if using HTTPS
    )

    # Create bucket if it doesn't exist
    bucket = MINIO_BUCKET_RAW
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    transaction_data_daily.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("date") \
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(f"s3a://{bucket}/daily/")
        
    logger.info(f"Data written to MinIO bucket: {bucket}/features")
    logger.info("Get transaction data completed successfully.")

if __name__ == "__main__":
    main()