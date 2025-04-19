import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession

dotenv_path = os.path.join("/opt/airflow/external_scripts/", '.env')  # Assuming .env is in the same directory
load_dotenv(dotenv_path)

# Load environment variables
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

# Create a spark session with Delta Lake and S3A support
def init_spark(spark_app_name:str):
    """
    This function initializes a Spark session with Delta Lake and S3A support.
    It also configures the session to use local mode and sets the timezone to UTC.
    The function returns the initialized Spark session.
    Args:
        spark_app_name: Name of the Spark session

    Returns:
        Spark session object
    """
    spark = (SparkSession.builder \
        .appName(spark_app_name) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.261,"
                "org.postgresql:postgresql:42.5.1") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{S3_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        
        # For better performance
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.fallback.enabled", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .getOrCreate())
    
    print("Spark session initialized")
    return spark

# Create a log file and set up logging
def get_logger(name: str, log_file: str) -> logging.Logger:
    """
    Creates a logger that writes to a file.

    Args:
        name: The name of the logger.
        log_file: The path to the log file.

    Returns:
        A logger that writes to the specified file.
    """

    # Check if the directory exists, and create it if it doesn't
    log_dir = os.path.dirname(log_file)  # Extract the directory part from the log_file path
    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir, exist_ok=True)  # Create the directory and any necessary parent directories
            print(f"Created log directory: {log_dir}")  # Optional: Print a message to the console
        except OSError as e:
            print(f"Error creating log directory: {e}")
            # Optionally raise the exception or handle it in another way
            raise

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)  # Or whatever level you want

    # Create a file handler and set the formatter
    file_handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    return logger
