import pytest
import os
import datetime as dt
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from scripts.utils import init_spark
from scripts.step_2_calculate_features import calculate_lxw_fts, main, MINIO_BUCKET_RAW, FEATURES_PATH


@pytest.fixture(scope="module")
def spark():
    # Initialize a local SparkSession for testing
    spark = init_spark("test_feature_calc", local_mode=True)
    yield spark
    spark.stop()


@pytest.fixture
def sample_transaction_data(spark):
    bucket_raw_fts = MINIO_BUCKET_RAW
    # Get transaction data from raw data source or create a sample DataFrame
    user_id_list = []

    date_start = '2025-03-10'
    date_end = dt.strptime(date_start, "%Y-%m-%d") + dt.timedelta(days=7)  # Assuming 7 days of data for testing
    date_end = dt.datetime.strftime(date_end, "%Y-%m-%d")

    df = spark.load.format("delta").load(f"s3a://{bucket_raw_fts}/daily/")\
                    .where(f'''user_id in tuple({user_id_list}) and date between "{date_start}" and "date_end"''')
    return df


def test_feature_calculation_logic(spark, sample_transaction_data):
    """Test that feature calculation logic produces expected results."""
    # Save sample data to a temporary location
    temp_path = "temp_data"
    date_test = "2025-03-17"
    sample_df = sample_transaction_data(spark)

    sample_df.write.format("delta").mode("overwrite").save("s3a://{temp_path}/daily/")
    sample_df = spark.read.format("delta").load(f"s3a://{temp_path}/daily/")

    # Calculate expected results manually
    time_window = "l1w"
    expected = calculate_lxw_fts(sample_df, time_window=time_window)

    expected_results = expected.collect()
    
    # Mock dependencies
    with patch("scripts.step_2_calculate_features.init_spark", return_value=spark), \
         patch("scripts.step_2_calculate_features.sys.argv", ["script.py", date_test]), \
         patch("scripts.step_2_calculate_features.get_logger"), \
         patch("scripts.step_2_calculate_features.DATA_PATH", temp_path), \
         patch("scripts.step_2_calculate_features.FEATURES_PATH", "temp_features"), \
         patch("scripts.step_2_calculate_features.Minio") as mock_minio:
        
        # Mock MinIO client
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = True
        mock_minio.return_value = mock_client
        
        # Run the main function
        main(date_test)
        
        # Read the output and verify
        result_df = spark.read.format("delta").load(FEATURES_PATH)\
                              .filter(F.col("date") == date_test)\
                              .join(expected.select('user_id'), how="inner", on="user_id")

        actual_results = result_df.collect()
        
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

def test_date_validation(spark):
    """Test that date validation works correctly."""
    with patch("scripts.step_2_calculate_features.init_spark", return_value=spark), \
         patch("scripts.step_2_calculate_features.sys.argv", ["script.py", "invalid-date"]), \
         patch("scripts.step_2_calculate_features.get_logger") as mock_logger, \
         pytest.raises(SystemExit) as excinfo:
        
        # Run with invalid date format
        main("invalid-date")
        
        # Verify it exits with error code 1
        assert excinfo.value.code == 1
        mock_logger.return_value.error.assert_called()