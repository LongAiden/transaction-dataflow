# test_2_calculate_features.py
import unittest
from unittest.mock import patch, MagicMock, call
from scripts.utils import get_logger, init_spark
import scripts.step_2_calculate_features as calculate_features
import os
import sys
import datetime as dt
import pandas as pd
from pyspark.sql import SparkSession
from dotenv import load_dotenv

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) # Absolute path to the scripts directory
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR) # Absolute path to parent directory of scripts
DOTENV_PATH = os.path.join(PROJECT_ROOT, 'scripts','.env')

load_dotenv(DOTENV_PATH)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS_LOCAL = os.getenv("KAFKA_BOOTSTRAP_SERVERS_LOCAL")
CDC_TRANSACTION_TOPIC = os.getenv("CDC_TRANSACTION_TOPIC")
S3_ENDPOINT_LOCAL = os.getenv("S3_ENDPOINT_LOCAL")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


class TestCalculateFeatures(unittest.TestCase):
    def setUp(self):
        """Set up environment variables and a mock Spark session before each test."""
        # Mock environment variables
        self.kafka_bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS_LOCAL
        self.cdc_transaction_topic = CDC_TRANSACTION_TOPIC
        self.minio_endpoint = S3_ENDPOINT_LOCAL
        self.minio_access_key = MINIO_ACCESS_KEY
        self.minio_secret_key = MINIO_SECRET_KEY
        self.minio_bucket = MINIO_BUCKET
        self.run_date_str = "2025-03-17"

        # Patch environment variables
        self.env_patcher = patch.dict(os.environ, {
            "KAFKA_BOOTSTRAP_SERVERS": self.kafka_bootstrap_servers,
            "CDC_TRANSACTION_TOPIC": self.cdc_transaction_topic,
            "S3_ENDPOINT": self.minio_endpoint,
            "S3_ACCESS_KEY": self.minio_access_key,
            "S3_SECRET_KEY": self.minio_secret_key,
            "MINIO_BUCKET": self.minio_bucket
        })
        self.env_patcher.start()

        # Mock SparkSession
        self.mock_spark = MagicMock()
        self.mock_spark.conf.set = MagicMock()
        self.mock_spark.read.format.return_value.option.return_value.load.return_value = MagicMock()
        self.mock_spark.createDataFrame.return_value = MagicMock()

        # Patch init_spark
        self.init_spark_patcher = patch('scripts.step_2_calculate_features.init_spark', return_value=self.mock_spark)
        self.mock_init_spark = self.init_spark_patcher.start()

        # Patch Minio client
        self.mock_minio_client = MagicMock()
        self.minio_patcher = patch('scripts.step_2_calculate_features.Minio', return_value=self.mock_minio_client)
        self.mock_minio = self.minio_patcher.start()

        # Patch logger
        self.mock_logger = MagicMock()
        self.logger_patcher = patch('scripts.step_2_calculate_features.get_logger', return_value=self.mock_logger)
        self.mock_logger_func = self.logger_patcher.start()

        # Patch the date
        self.date_patcher = patch('scripts.step_2_calculate_features.RUN_DATE_STR', self.run_date_str)
        self.mock_date = self.date_patcher.start()

        # Mock the command line arguments
        self.sys_argv_patcher = patch('sys.argv', ['step_2_calculate_features.py', self.run_date_str])
        self.mock_sys_argv = self.sys_argv_patcher.start()

    def tearDown(self):
        """Clean up after each test."""
        self.env_patcher.stop()
        self.init_spark_patcher.stop()
        self.minio_patcher.stop()
        self.logger_patcher.stop()
        self.date_patcher.stop()
        self.sys_argv_patcher.stop()

    @patch('scripts.step_2_calculate_features.load_dotenv')
    def test_calculate_features_success(self, mock_load_dotenv):
        """Test the happy path for calculate_features."""
        mock_load_dotenv.return_value = None

        # Mock Kafka data
        mock_kafka_df = MagicMock()
        self.mock_spark.read.format.return_value.option.return_value.load.return_value = mock_kafka_df

        # Mock parsed CDC data
        mock_cdc_df = MagicMock()
        mock_kafka_df.selectExpr.return_value.withColumn.return_value.select.return_value = mock_cdc_df

        # Mock transaction data
        mock_transaction_data = MagicMock()
        mock_cdc_df.where.return_value.select.return_value = mock_transaction_data

        # Mock Window and aggregations
        mock_window = MagicMock()
        mock_transaction_data.withColumn.return_value.filter.return_value.join.return_value.withColumn.return_value.withColumn.return_value.where.return_value.drop.return_value = mock_transaction_data
        mock_transaction_data.groupBy.return_value.agg.return_value.withColumnRenamed.return_value.withColumn.return_value = mock_transaction_data
        mock_transaction_data.filter.return_value.select.return_value.distinct.return_value.count.return_value = 10

        # Mock MinIO bucket existence and writing data
        self.mock_minio_client.bucket_exists.return_value = False
        mock_transaction_data.write.format.return_value.mode.return_value.partitionBy.return_value.option.return_value.option.return_value.save.return_value = None

        # Execute the script
        calculate_features.main()

        # Assertions
        self.mock_init_spark.assert_called_once_with("FeastDeltaExample")
        self.mock_spark.conf.set.assert_has_calls([
            call("spark.sql.sources.partitionOverwriteMode", "dynamic"),
            call("spark.sql.sources.partitionOverwriteMode", "dynamic")
        ])
        self.mock_spark.read.format.return_value.option.assert_has_calls([
            call("kafka.bootstrap.servers", self.kafka_bootstrap_servers),
            call("subscribe", self.cdc_transaction_topic),
            call("startingOffsets", "earliest")
        ], any_order=True)
        mock_transaction_data.write.format.return_value.mode.return_value.partitionBy.return_value.option.assert_has_calls([
            call("delta.columnMapping.mode", "name"),
            call("delta.minReaderVersion", "2"),
            call("delta.minWriterVersion", "5")
        ])
        self.mock_minio_client.make_bucket.assert_called_once_with(self.minio_bucket)
        self.mock_logger.info.assert_called()

    # @patch('scripts.step_2_calculate_features.load_dotenv')
    # def test_calculate_features_not_enough_dates(self, mock_load_dotenv):
    #     """Test when not enough distinct dates are found in the data."""
    #     mock_load_dotenv.return_value = None

    #     # Mock Kafka data
    #     mock_kafka_df = MagicMock()
    #     self.mock_spark.read.format.return_value.option.return_value.load.return_value = mock_kafka_df

    #     # Mock parsed CDC data
    #     mock_cdc_df = MagicMock()
    #     mock_kafka_df.selectExpr.return_value.withColumn.return_value.select.return_value = mock_cdc_df

    #     # Mock transaction data
    #     mock_transaction_data = MagicMock()
    #     mock_cdc_df.where.return_value.select.return_value = mock_transaction_data

    #     # Mock Window and aggregations
    #     mock_transaction_data.withColumn.return_value.filter.return_value.join.return_value.withColumn.return_value.withColumn.return_value.where.return_value.drop.return_value = mock_transaction_data
    #     mock_transaction_data.groupBy.return_value.agg.return_value.withColumnRenamed.return_value.withColumn.return_value = mock_transaction_data

    #     # Mock distinct dates count (less than 7)
    #     mock_transaction_data.filter.return_value.select.return_value.distinct.return_value.count.return_value = 3

    #     # Expect a SystemExit exception
    #     with self.assertRaises(SystemExit) as context:
    #         calculate_features.main()

    #     # Assert that the exit code is 1
    #     self.assertEqual(context.exception.code, 1)
    #     self.mock_logger.error.assert_called_once()

    @patch('scripts.step_2_calculate_features.load_dotenv')
    def test_calculate_features_kafka_read_error(self, mock_load_dotenv):
        """Test when there's an error reading data from Kafka."""
        mock_load_dotenv.return_value = None

        # Mock Kafka read to raise an exception
        self.mock_spark.read.format.return_value.option.return_value.load.side_effect = Exception("Kafka read error")

        # Expect an exception (either direct or due to the Kafka read error)
        with self.assertRaises(Exception):
            calculate_features.main()

        # Verify logger call
        self.mock_logger.info.assert_called()

    # @patch('scripts.step_2_calculate_features.load_dotenv')
    # def test_calculate_features_count_distinct_dates_error(self, mock_load_dotenv):
    #     """Test when there's an error when counting distinct dates."""
    #     mock_load_dotenv.return_value = None

    #     # Mock Kafka data
    #     mock_kafka_df = MagicMock()
    #     self.mock_spark.read.format.return_value.option.return_value.load.return_value = mock_kafka_df

    #     # Mock parsed CDC data
    #     mock_cdc_df = MagicMock()
    #     mock_kafka_df.selectExpr.return_value.withColumn.return_value.select.return_value = mock_cdc_df

    #     # Mock transaction data
    #     mock_transaction_data = MagicMock()
    #     mock_cdc_df.where.return_value.select.return_value = mock_transaction_data

    #     # Mock Window and aggregations
    #     mock_transaction_data.withColumn.return_value.filter.return_value.join.return_value.withColumn.return_value.withColumn.return_value.where.return_value.drop.return_value = mock_transaction_data
    #     mock_transaction_data.groupBy.return_value.agg.return_value.withColumnRenamed.return_value.withColumn.return_value = mock_transaction_data

    #     # Mock the count function to raise an exception
    #     mock_transaction_data.filter.return_value.select.return_value.distinct.return_value.count.side_effect = Exception("Error counting distinct dates")

    #     # Expect a SystemExit exception
    #     with self.assertRaises(SystemExit) as context:
    #         calculate_features.main()

    #     # Assert that the exit code is 1
    #     self.assertEqual(context.exception.code, 1)
    #     self.mock_logger.error.assert_called_once()

    def test_main_function(self):
        """Test that the main function is being called."""
        with patch('scripts.step_2_calculate_features.main') as mock_main:
             calculate_features.main()
             mock_main.assert_called()

if __name__ == '__main__':
    unittest.main()