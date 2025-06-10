import pytest
import os
import datetime as dt
import logging
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from scripts.utils import init_spark, get_logger
from minio import Minio
from dotenv import load_dotenv
from scripts.step_2_calculate_features import calculate_lxw_fts, main

dotenv_path = os.path.join("./scripts/", '.env')  # Assuming .env is in the same directory
load_dotenv(dotenv_path)

# Environment variables
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT_LOCAL")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW")
DATA_PATH = f"s3a://{MINIO_BUCKET_RAW}/daily/"
FEATURES_PATH = f"s3a://{MINIO_BUCKET}/features/"

# Logging setup
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE = os.path.join(LOG_DIR, "test_step_2_calculate_fts.log")

logger = get_logger(__name__, LOG_FILE)

@pytest.fixture(scope="module")
def spark():
    logger.info("Starting Spark session for testing...")
    # Initialize a local SparkSession for testing
    spark = init_spark("test_feature_calc", local_mode=True)
    yield spark
    spark.stop()


def test_minio_connection():
    """Test the connection to MinIO."""
    logger.info("Starting test_minio_connection...")

    try:
        # Initialize MinIO client
        minio_client = Minio(
            MINIO_ENDPOINT,  # Your MinIO server endpoint
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False  # Set to True if using HTTPS
        )

        # Attempt to list buckets to test the connection
        buckets = minio_client.list_buckets()
        logger.info(f"Successfully connected to MinIO. Buckets found: {buckets}")
        assert isinstance(buckets, list), "Expected a list of buckets from MinIO"

    except Exception as e:
        logger.error(f"Error connecting to MinIO: {e}")
        pytest.fail(f"Could not connect to MinIO: {e}")

    logger.info("test_minio_connection completed successfully.")


def test_feature_calculation_logic(spark):
    """Test that feature calculation logic produces expected results."""
    logger.info("Starting test_feature_calculation_logic...")

    # Save sample data to a temporary location
    temp_path = "temp-data"
    date_test = "2025-03-17"
    user_id_list = ['user_000066','user_000424','user_000098']

    date_start = '2025-03-10'
    date_end = dt.datetime.strptime(date_start, "%Y-%m-%d") + dt.timedelta(days=7)  # Assuming 7 days of data for testing
    date_end = dt.datetime.strftime(date_end, "%Y-%m-%d")

    logger.info(f"Loading sample data from: s3a://{MINIO_BUCKET_RAW}/daily/")
    sample_df = spark.read.format("delta").load(f"s3a://{MINIO_BUCKET_RAW}/daily/")\
                    .where(f'''`User ID` in {tuple(user_id_list)} and date between "{date_start}" and "{date_end}"''')

    logger.info(f"Loaded {sample_df.count()} rows for testing.")

    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,  # Your MinIO server endpoint
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Set to True if using HTTPS
    )

    try:
        logger.info(f"Checking if bucket exists: {temp_path}")
        if not minio_client.bucket_exists(temp_path):
            logger.info(f"Creating bucket: {temp_path}")
            minio_client.make_bucket(temp_path)
            logger.info(f"Bucket created: {temp_path}")
    except Exception as e:
        logger.error(f"Error ensuring temporary bucket exists: {e}")
        pytest.fail(f"Could not ensure temporary bucket exists: {e}")

    try:
        logger.info(f"Saving sample data to: s3a://{temp_path}/daily/")
        sample_df.write.format("delta") .mode("overwrite") \
            .partitionBy("date") \
            .option("delta.columnMapping.mode", "name") \
            .option("delta.minReaderVersion", "2") \
            .option("delta.minWriterVersion", "5")\
            .save(f"s3a://{temp_path}/daily/")
        logger.info("Sample data saved successfully.")
    except Exception as e:
         logger.error(f"Error saving sample data to temporary path: {e}")
         pytest.fail(f"Could not save sample data to temporary path: {e}")

    try:
        logger.info(f"Reading sample data back from: s3a://{temp_path}/daily/")
        sample_df = spark.read.format("delta").load(f"s3a://{temp_path}/daily/")
        logger.info(f"Successfully read {sample_df.count()} rows from temporary path.")
    except Exception as e:
        logger.error(f"Error reading sample data from temporary path: {e}")
        pytest.fail(f"Could not read sample data from temporary path: {e}")

    # Calculate expected results manually
    logger.info("Calculating expected results...")
    time_window = "l1w"
    expected = calculate_lxw_fts(sample_df, time_window=time_window)

    expected_results = expected.collect()
    logger.info(f"Calculated expected results: {len(expected_results)} rows.")

    # Mock dependencies
    with patch("scripts.step_2_calculate_features.init_spark", return_value=spark), \
         patch("scripts.step_2_calculate_features.sys.argv", ["script.py", date_test]), \
         patch("scripts.step_2_calculate_features.get_logger", return_value=logger), \
         patch("scripts.step_2_calculate_features.DATA_PATH", temp_path), \
         patch("scripts.step_2_calculate_features.FEATURES_PATH", "temp_features"), \
         patch("scripts.step_2_calculate_features.Minio") as mock_minio:

        # Mock MinIO client
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = True
        mock_minio.return_value = mock_client

        # # Run the main function
        # main(date_test)

        # Read the output and verify
        logger.info(f"Reading output from: {FEATURES_PATH}")
        result_df = spark.read.format("delta").load(FEATURES_PATH)\
                              .filter(F.col("date") == date_test)\
                              .join(expected.select('user_id'), how="inner", on="user_id")

        actual_results = result_df.collect()
        logger.info(f"Actual results count: {len(actual_results)}")

        # Compare the results
        assert len(actual_results) == len(expected_results)

        # Convert to dictionaries for easier comparison
        expected_dict = {row["user_id"]: row for row in expected_results}
        actual_dict = {row["user_id"]: row for row in actual_results}

        # Verify each user's features
        for user_id in expected_dict:
            assert user_id in actual_dict

            expected_row = expected_dict[user_id]
            actual_row = actual_dict[user_id]

            assert actual_row["num_transactions_l1w"] == expected_row["num_transactions_l1w"]
            assert actual_row["total_amount_l1w"] == expected_row["total_amount_l1w"]
            assert abs(actual_row["avg_amount_l1w"] - expected_row["avg_amount_l1w"]) < 0.001
            assert actual_row["min_amount_l1w"] == expected_row["min_amount_l1w"]
            assert actual_row["max_amount_l1w"] == expected_row["max_amount_l1w"]
            assert actual_row["num_vendors_l1w"] == expected_row["num_vendors_l1w"]
            assert actual_row["num_sources_l1w"] == expected_row["num_sources_l1w"]

    logger.info("test_feature_calculation_logic completed successfully.")


def test_date_validation(spark, caplog):
    """Test that date validation works correctly."""
    logger.info("Starting test_date_validation...")
    with caplog.at_level(logging.ERROR): # Capture ERROR level logs for the assertion
        with patch("scripts.step_2_calculate_features.init_spark", return_value=spark), \
             patch("scripts.step_2_calculate_features.sys.argv", ["script.py", "invalid-date"]), \
             patch("scripts.step_2_calculate_features.get_logger", return_value=logger), \
             pytest.raises(SystemExit) as excinfo:

            # Run with invalid date format
            logger.info("Testing invalid date validation...") # This info log won't be captured by caplog
            from scripts.step_2_calculate_features import main # Import here if needed within the patch
            main("invalid-date") # Assuming main accepts the date string directly

        # Verify it exits with error code 1
        assert excinfo.value.code == 1
        logger.info("SystemExit with code 1 verified.")

        # Verify an error message was logged about the invalid date
        # Check if any log record contains the expected error message substring
        error_logged = any("Invalid date format" in record.message for record in caplog.records)
        assert error_logged, "Expected date format error message not found in logs"
        logger.info("Invalid date error message assertion passed.")

    logger.info("test_date_validation completed successfully.")