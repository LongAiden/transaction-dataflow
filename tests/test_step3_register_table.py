import pytest
import os
from unittest.mock import patch, MagicMock
from pathlib import Path
import tempfile
import shutil
from pyspark.sql import functions as F
from scripts.utils import init_spark

@pytest.fixture(scope="module")
def spark():
    # Initialize a local SparkSession for testing
    spark = init_spark("test_feast_register", local_mode=True)
    yield spark
    spark.stop()

@pytest.fixture
def sample_feature_data(spark):
    # Create sample feature data that would be the output of step_2
    data = [
        ("userA", 3, 60.0, 20.0, 10.0, 30.0, 2, 3, "2023-01-07"),
        ("userB", 2, 40.0, 20.0, 15.0, 25.0, 2, 2, "2023-01-07"),
        ("userC", 2, 40.0, 20.0, 5.0, 35.0, 2, 2, "2023-01-07"),
    ]
    
    df = spark.createDataFrame(
        data, 
        schema=[
            "user_id", 
            "num_transactions_l1w", 
            "total_amount_l1w", 
            "avg_amount_l1w", 
            "min_amount_l1w", 
            "max_amount_l1w", 
            "num_vendors_l1w", 
            "num_sources_l1w", 
            "date"
        ]
    )
    return df

@pytest.fixture
def temp_feature_store():
    # Create a temporary directory for the feature store
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up after test
    shutil.rmtree(temp_dir)

def test_feature_registration(spark, sample_feature_data, temp_feature_store):
    """Test feature registration with Feast."""
    # Save sample data to a temporary location
    temp_features_path = "temp_features"
    sample_feature_data.write.format("delta").mode("overwrite").partitionBy("date").save(temp_features_path)
    
    # Mock FeatureStore and Entity classes
    mock_feature_store = MagicMock()
    mock_entity = MagicMock()
    mock_feature_view = MagicMock()
    
    with patch("scripts.step_3_1_fs_register_table.init_spark", return_value=spark), \
         patch("scripts.step_3_1_fs_register_table.get_logger"), \
         patch("scripts.step_3_1_fs_register_table.FeatureStore", return_value=mock_feature_store), \
         patch("scripts.step_3_1_fs_register_table.Entity", return_value=mock_entity), \
         patch("scripts.step_3_1_fs_register_table.FeatureView", return_value=mock_feature_view), \
         patch("scripts.step_3_1_fs_register_table.DATA_PATH", temp_features_path), \
         patch.dict(os.environ, {
             "S3_ENDPOINT": "localhost:9000",
             "S3_ENDPOINT_LOCAL": "localhost:9000",
             "S3_ACCESS_KEY": "minioadmin",
             "S3_SECRET_KEY": "minioadmin",
             "MINIO_BUCKET": "feast-bucket"
         }):
        
        # Import the script here to ensure patched imports are used
        from scripts.step_3_1_fs_register_table import customer, customer_features
        
        # Verify that FeatureStore.apply was called with the right arguments
        assert mock_feature_store.apply.call_count == 2
        mock_feature_store.apply.assert_any_call(mock_entity)
        mock_feature_store.apply.assert_any_call(mock_feature_view)
        
        # Verify materialization was attempted
        assert mock_feature_store.materialize_incremental.call_count == 1

def test_feature_materialization_integration(spark, sample_feature_data, temp_feature_store):
    """Integration test for feature materialization with a real feature store."""
    # This is a more comprehensive test that creates an actual feature store
    # Save sample data to a temporary location
    temp_features_path = "temp_features"
    sample_feature_data.write.format("delta").mode("overwrite").partitionBy("date").save(temp_features_path)
    
    with patch("scripts.step_3_1_fs_register_table.repo_path", temp_feature_store), \
         patch("scripts.step_3_1_fs_register_table.init_spark", return_value=spark), \
         patch("scripts.step_3_1_fs_register_table.get_logger"), \
         patch("scripts.step_3_1_fs_register_table.DATA_PATH", temp_features_path), \
         patch.dict(os.environ, {
             "S3_ENDPOINT": "localhost:9000",
             "S3_ENDPOINT_LOCAL": "localhost:9000",
             "S3_ACCESS_KEY": "minioadmin", 
             "S3_SECRET_KEY": "minioadmin",
             "MINIO_BUCKET": "feast-bucket"
         }):
        
        # Create a feature_store.yaml file in the temp directory
        os.makedirs(os.path.join(temp_feature_store, "data"), exist_ok=True)
        with open(os.path.join(temp_feature_store, "feature_store.yaml"), "w") as f:
            f.write("""
project: test_project
registry: data/registry.db
provider: local
online_store:
    type: sqlite
    path: data/online_store.db
offline_store:
    type: spark
            """)
        
        try:
            # Run with actual Feast
            from scripts.step_3_1_fs_register_table import customer, customer_features
            from feast import FeatureStore
            
            # Create a feature store and apply entities and feature views
            fs = FeatureStore(temp_feature_store)
            fs.apply([customer, customer_features])
            
            # Check that registry has the entity and feature view
            registry = fs.registry
            assert "customer" in registry.list_entities()
            assert "customer_features" in registry.list_feature_views()
            
            # Check feature view schema
            fv = registry.get_feature_view("customer_features")
            assert len(fv.schema) == 7
            
        except ImportError:
            # Skip the test if Feast is not installed
            pytest.skip("Feast not installed, skipping integration test")