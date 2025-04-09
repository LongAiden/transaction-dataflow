import sys
import os
from utils import init_spark
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

RUN_DATE_STR = sys.argv[1]
RUN_DATE_STR_7DAYS = (dt.datetime.strptime(RUN_DATE_STR, "%Y-%m-%d") - dt.timedelta(days=7)).strftime('%Y-%m-%d')
print(RUN_DATE_STR_7DAYS, RUN_DATE_STR)

if __name__ == "__main__":
    dotenv_path = os.path.join("./scripts/", '.env') 
    load_dotenv(dotenv_path)

    # Environment variables
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    CDC_TRANSACTION_TOPIC = os.getenv("CDC_TRANSACTION_TOPIC")
    POSTGRES_CON_STR = os.getenv("POSTGRES_CON_STR")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    MINIO_ENDPOINT = os.getenv("S3_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET")

    print(KAFKA_BOOTSTRAP_SERVERS, CDC_TRANSACTION_TOPIC)
    print(MINIO_ENDPOINT, MINIO_BUCKET)

    # Create Spark session configured for MinIO (S3A)
    spark = init_spark("FeastDeltaExample")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    spark.conf.set("spark.jars.packages",  "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                "org.postgresql:postgresql:42.5.1")
    spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", 
                   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

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
    transaction_data = cdc_df.where('''op in ('c','u','r')''').select(
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
    transaction_data = transaction_data.withColumn("rank", F.row_number().over(w)) \
                            .filter(F.col("rank") == 1) \
                            .join(transaction_data_deleted, on=['id'], how="leftanti")\
                            .withColumn("timestamp", F.to_timestamp(F.col("Time"), "yyyy-MM-dd HH:mm:ss")) \
                            .withColumn("date", F.to_date(F.col("timestamp")))\
                            .where(f'''date between "{RUN_DATE_STR_7DAYS}" and "{RUN_DATE_STR}"''')\
                            .drop('timestamp', 'ts_ms','rank','id')

    transaction_data.show(5)

    # Read streaming data from Kafka topic "cdc_transaction"
    distinct_dates = transaction_data.select("date").distinct().count()
    
    if distinct_dates < 7:
        print(f"Error: Found only {distinct_dates} distinct dates in the data. Minimum required is 7 days.")
        sys.exit(1)
                
    # Calculate lxw features
    time_window = "l1w"
    agg_fts = transaction_data.groupBy("User ID")\
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
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(f"s3a://{bucket}/features")