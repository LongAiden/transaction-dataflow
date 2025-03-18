import sys
import os
import json
import logging
import datetime as dt
import pandas as pd
import numpy as np
from kafka import KafkaProducer 
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from dotenv import load_dotenv
from minio import Minio

dotenv_path = os.path.join("/opt/airflow/external_scripts/", '.env')  # Assuming .env is in the same directory
load_dotenv(dotenv_path)

RUN_DATE_STR = sys.argv[1]
RUN_DATE_STR_7DAYS = (dt.datetime.strptime(RUN_DATE_STR, "%Y-%m-%d") - dt.timedelta(days=7)).strftime('%Y-%m-%d')

if __name__ == "__main__":
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW")
    MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET")

    print(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    print(MINIO_ENDPOINT, MINIO_BUCKET)   

    # Create Spark session configured for MinIO (S3A)
    spark = SparkSession.builder \
        .appName("Weekly_Monthly_Feature_Calculation") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "io.delta:delta-core_2.12:2.4.0," # previosly 2.4.0
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://"+MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[*]")\
            .getOrCreate()

    # Read streaming data from Kafka topic "crypto_data"
    raw_kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()


    # Define the schema for your JSON data (adjust field names and types as needed)
    schema = StructType([
        StructField("User ID", StringType(), True),
        StructField("Transaction ID", StringType(), True),
        StructField("Amount", DoubleType(), True),
        StructField("Vendor", StringType(), True),
        StructField("Sources", StringType(), True),
        StructField("Time", StringType(), True)
    ])

    # Parse the JSON from the Kafka "value" column (which is binary)
    json_df = raw_kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")).select("data.*")

    # Convert the string date (agg_date) to a proper date type (specify the format if needed)
    json_df = json_df.withColumn("timestamp", F.to_timestamp(F.col("Time"), "yyyy-MM-dd HH:mm:ss"))\
                     .withColumn("date", F.to_date(F.col("timestamp"), "yyyy-MM-dd"))\
                     .where(f'''date between "{RUN_DATE_STR_7DAYS}" and "{RUN_DATE_STR}"''')\
                     .drop('timestamp')
    
    distinct_dates = json_df.select("date").distinct().count()
    
    if distinct_dates < 7:
        print(f"Error: Found only {distinct_dates} distinct dates in the data. Minimum required is 7 days.")
        sys.exit(1)
                
    # Calculate lxw features
    time_window = "l1w"
    agg_fts = json_df.groupBy("User ID")\
                     .agg(F.count("Transaction ID").alias(f"num_transactions_{time_window}"),
                          F.sum("Amount").alias(f"total_amount_{time_window}"),
                          F.avg("Amount").alias(f"avg_amount_{time_window}"),
                          F.min("Amount").alias(f"min_amount_{time_window}"),
                          F.max("Amount").alias(f"max_amount_{time_window}"),
                          F.countDistinct("Vendor").alias(f"num_vendors_{time_window}"),
                          F.countDistinct("Sources").alias(f"num_sources_{time_window}"))
    
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
        .mode("overwrite")\
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(f"s3a://{bucket}/features/")