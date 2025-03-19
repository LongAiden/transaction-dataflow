import requests
import json
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def register_connector():
    # Wait for Kafka Connect to be ready
    time.sleep(30)
    
    # Load connector configuration
    with open('/opt/airflow/config/postgres-connector.json', 'r') as f:
        connector_config = json.load(f)
    
    # Register connector
    response = requests.post(
        'http://kafka-connect:8083/connectors',
        headers={'Content-Type': 'application/json'},
        data=json.dumps(connector_config)
    )
    
    if response.status_code == 201:
        print("Connector registered successfully")
    else:
        print(f"Failed to register connector: {response.text}")

def create_spark_session():
    return SparkSession.builder \
        .appName("CDC Processing") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "io.debezium:debezium-core:2.3.0.Final,"
                "io.debezium:debezium-connector-postgres:2.3.0.Final") \
        .getOrCreate()

def process_cdc_events():
    spark = create_spark_session()
    
    # Define schema for CDC events
    schema = StructType([
        StructField("before", StringType(), True),
        StructField("after", StringType(), True),
        StructField("source", StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), True),
            StructField("table", StringType(), True),
            StructField("server_id", StringType(), True),
            StructField("gtid", StringType(), True),
            StructField("file", StringType(), True),
            StructField("pos", StringType(), True),
            StructField("row", StringType(), True),
            StructField("thread", StringType(), True),
            StructField("query", StringType(), True)
        ]), True),
        StructField("op", StringType(), True),
        StructField("ts_ms", TimestampType(), True)
    ])

    # Read CDC events from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "dbserver1.public.users") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON payload
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("value")
    ).select("value.*")

    # Process different operations (INSERT, UPDATE, DELETE)
    query = parsed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    register_connector()
    process_cdc_events()