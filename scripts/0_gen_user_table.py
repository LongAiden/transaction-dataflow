import sys
import os
import datetime as dt
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from dotenv import load_dotenv
from minio import Minio

dotenv_path = os.path.join("./scripts", '.env')
load_dotenv(dotenv_path)

def generate_user_data(num_rows=10000):
    """Generates a Pandas DataFrame with pseudo transaction data."""

    # User IDs
    user_ids = [f"user_{i:06d}" for i in np.random.choice(num_rows, size=num_rows)]

    # Transaction IDs
    age = np.random.randint(22, 90, size=num_rows)

    # Source
    gender = np.random.choice(["M", "F", "Other"], 
                            size=num_rows, p=[0.5, 0.4, 0.1])

    # Amounts
    occupation = np.random.choice(["M", "F", "Other"], 
                            size=num_rows, p=[0.5, 0.4, 0.1])

    # location
    location = np.random.choice(["City A", "City B", "City C", "City D", "City E", 
                                "City F", "City G", "City H", "Unknown"], 
                            size=num_rows, p=[0.1,0.12,0.08,0.1,0.15,0.05,0.1,0.1,0.2])
    
    # Day start
    start = dt.datetime(2020, 1, 1)
    end = dt.datetime(2025, 3, 1)
    
    # Calculate total days between start and end
    days_between = (end - start).days
    
    # Generate random days and add to start date
    random_days = np.random.randint(0, days_between, size=num_rows)
    day_start = [(start + dt.timedelta(days=int(x))).strftime('%Y-%m-%d') for x in random_days]

    # Create DataFrame
    df = pd.DataFrame({
        "user_id": user_ids,
        "age": age,
        "gender": gender,
        "location": location,
        "occupation": occupation,
        "day_start": day_start,
        
    })

    return df

if __name__ == "__main__":
    MINIO_ENDPOINT_LOCAL = os.getenv("S3_ENDPOINT_LOCAL")
    MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    MINIO_BUCKET_USER = os.getenv("MINIO_BUCKET_USER")

    print(MINIO_ENDPOINT_LOCAL, MINIO_BUCKET_USER)   

    # Generate and print the data
    customer_df = generate_user_data(num_rows=5000)
    print(customer_df.head())

    # Create Spark session configured for MinIO (S3A)
    spark = SparkSession.builder \
        .appName("Weekly_Monthly_Feature_Calculation") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://"+MINIO_ENDPOINT_LOCAL) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[*]")\
            .getOrCreate()
    
    # user_id  age gender location occupation   day_start
    # Define the schema for customer df
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("location", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("day_start", StringType(), True),
    ])

    customer_df = spark.createDataFrame(customer_df, schema)
    customer_df = customer_df.withColumn("day_start", F.to_date(F.col("day_start"), "yyyy-MM-dd"))
    customer_df.show(5)


    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT_LOCAL,  # Your MinIO server endpoint
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Set to True if using HTTPS
    )

    # Create bucket if it doesn't exist
    bucket = MINIO_BUCKET_USER
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    customer_df.write \
        .format("delta") \
        .mode("overwrite")\
        .option("delta.columnMapping.mode", "name") \
        .option("delta.minReaderVersion", "2") \
        .option("delta.minWriterVersion", "5") \
        .save(f"s3a://{bucket}/demographic/")
    
    print(f"Data saved to MinIO in {MINIO_BUCKET_USER}!")