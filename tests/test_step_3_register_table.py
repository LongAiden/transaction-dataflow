import os
import pytest
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch, MagicMock
from pyspark.sql import functions as F
from scripts.utils import init_spark, get_logger
from minio import Minio
from dotenv import load_dotenv

# Load environment variables from .env file
dotenv_path = os.path.join("./scripts/", '.env') 
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
LOG_FILE = os.path.join(LOG_DIR, "test_step_3_register_table.log")
logger = get_logger(__name__, LOG_FILE)

TEMP_FEATURES_PATH = "temp-features"

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

def test_feature_registration(spark, sample_feature_data):
    """Test feature registration with Feast."""
    # Save sample data to a temporary location
    temp_features_path = TEMP_FEATURES_PATH
    
    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,  # Your MinIO server endpoint
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Set to True if using HTTPS
    )

    try:
        logger.info(f"Checking if bucket exists: {temp_features_path}")
        if not minio_client.bucket_exists(temp_features_path):
            logger.info(f"Creating bucket: {temp_features_path}")
            minio_client.make_bucket(temp_features_path)
            logger.info(f"Bucket created: {temp_features_path}")
    except Exception as e:
        logger.error(f"Error ensuring temporary bucket exists: {e}")
        pytest.fail(f"Could not ensure temporary bucket exists: {e}")
        
    sample_feature_data.write.format("delta").mode("overwrite")\
                        .partitionBy("date").save(f"s3a://{temp_features_path}/")
    
    # Mock FeatureStore and Entity classes
    logger.info("Createing Mock")
    mock_feature_store = MagicMock()
    mock_entity = MagicMock()
    mock_feature_view = MagicMock()
    
    with patch("scripts.step_3_1_fs_register_table.init_spark", return_value=spark), \
         patch("scripts.step_3_1_fs_register_table.get_logger"), \
         patch("scripts.step_3_1_fs_register_table.FeatureStore", return_value=mock_feature_store), \
         patch("scripts.step_3_1_fs_register_table.Entity", return_value=mock_entity), \
         patch("scripts.step_3_1_fs_register_table.FeatureView", return_value=mock_feature_view), \
         patch("scripts.step_3_1_fs_register_table.DATA_PATH", temp_features_path):
        
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
    temp_features_path = TEMP_FEATURES_PATH
    sample_feature_data.write.format("delta").mode("overwrite")\
                        .partitionBy("date").save(f's3a://{temp_features_path}/')
    
    with patch("scripts.step_3_1_fs_register_table.repo_path", temp_feature_store), \
         patch("scripts.step_3_1_fs_register_table.init_spark", return_value=spark), \
         patch("scripts.step_3_1_fs_register_table.get_logger"), \
         patch("scripts.step_3_1_fs_register_table.DATA_PATH", temp_features_path):
        
        # Create a feature_store.yaml file in the temp directory
        os.makedirs(os.path.join(temp_feature_store, "data_test"), exist_ok=True)
        with open(os.path.join(temp_feature_store, "feature_store.yaml"), "w") as f:
            f.write("""
                    project: test_project
                    registry: data_test/registry.db
                    provider: local
                    online_store:
                        type: sqlite
                        path: data_test/online_store.db
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
        except Exception as e:
            logger.error(f"An error occurred during the integration test: {e}")
            raise  # Re-raise the exception to fail the test