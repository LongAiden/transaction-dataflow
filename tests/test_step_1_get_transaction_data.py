import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import Row
import datetime
import os

class TestGetTransactionData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestGetTransactionData") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch('scripts.step_1_2_get_transaction_data.init_spark')
    @patch('scripts.step_1_2_get_transaction_data.get_logger')
    @patch('scripts.step_1_2_get_transaction_data.Minio')
    def test_main_end_to_end(self, mock_minio, mock_get_logger, mock_init_spark):
        # Mocking dependencies
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_init_spark.return_value = self.spark
        mock_minio_client = MagicMock()
        mock_minio.return_value = mock_minio_client
        mock_minio_client.bucket_exists.return_value = True

        # Mocking Kafka data (replace with realistic data)
        mock_kafka_data = [
            Row(value='{"schema": {"type": "struct", "fields": [], "optional": false, "name": "name", "version": 1}, "payload": {"before": null, "after": {"id": 1, "User ID": "user1", "Transaction ID": "tx1", "Amount": 100.0, "Vendor": "vendor1", "Sources": "source1", "Time": "2024-01-01 10:00:00"}, "source": {"version": "1.0", "connector": "postgres", "name": "dbserver", "ts_ms": 1672531200000, "snapshot": "true", "db": "mydb", "sequence": "seq1", "schema": "public", "table": "transactions", "txId": 123, "lsn": 456, "xmin": 789}, "op": "c", "ts_ms": 1672531200000, "transaction": {"id": "tx1", "total_order": 1, "data_collection_order": 1}}}'),
            Row(value='{"schema": {"type": "struct", "fields": [], "optional": false, "name": "name", "version": 1}, "payload": {"before": {"id":1, "User ID": "user1", "Transaction ID": "tx1", "Amount": 100.0, "Vendor": "vendor1", "Sources": "source1", "Time": "2024-01-01 10:00:00"}, "after": null, "source": {"version": "1.0", "connector": "postgres", "name": "dbserver", "ts_ms": 1672531200000, "snapshot": "true", "db": "mydb", "sequence": "seq1", "schema": "public", "table": "transactions", "txId": 123, "lsn": 456, "xmin": 789}, "op": "d", "ts_ms": 1672531200000, "transaction": {"id": "tx1", "total_order": 1, "data_collection_order": 1}}}')
        ]
        mock_kafka_df = self.spark.createDataFrame(mock_kafka_data)

        # Mock reading from Kafka
        mock_kafka_reader = MagicMock()
        mock_kafka_reader.format.return_value = mock_kafka_reader
        mock_kafka_reader.option.return_value = mock_kafka_reader
        mock_kafka_reader.load.return_value = mock_kafka_df

        self.spark.read = mock_kafka_reader

        # Mock environment variables
        with patch.dict(os.environ, {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "CDC_TRANSACTION_TOPIC": "test_topic",
            "S3_ENDPOINT": "s3.amazonaws.com",
            "S3_ACCESS_KEY": "test_access_key",
            "S3_SECRET_KEY": "test_secret_key",
            "MINIO_BUCKET_RAW": "test_bucket"
        }):
            # Call the main function
            from scripts.step_1_2_get_transaction_data import main
            RUN_DATE_STR = "2024-01-01"
            main(RUN_DATE_STR)

        # Assertions
        mock_init_spark.assert_called_once()
        mock_get_logger.assert_called_once()
        mock_minio.assert_called_once()
        mock_minio_client.bucket_exists.assert_called_once()
        # Add assertions to check if the data is written to MinIO correctly

if __name__ == '__main__':
    unittest.main()