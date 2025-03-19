import requests
import json
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

def check_connector_exists():
    try:
        response = requests.get('http://kafka-connect:8083/connectors/postgres-connector')
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def register_connector():
    # Wait for Kafka Connect to be ready
    time.sleep(30)
    
    # Check if connector already exists
    if check_connector_exists():
        print("Connector already registered, skipping registration")
        return
    
    # Load connector configuration
    config_path = '/opt/airflow/config/config_debezium.json'
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    with open(config_path, 'r') as f:
        connector_config = json.load(f)
    
    try:
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
            if response.status_code == 409:
                print("Connector already exists")
    except requests.exceptions.ConnectionError:
        print("Failed to connect to Kafka Connect. Make sure the service is running.")
        raise

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