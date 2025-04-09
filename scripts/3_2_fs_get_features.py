from pyspark.sql import SparkSession
from feast import FeatureStore
from pathlib import Path
import pandas as pd
from utils import init_spark

# Initialize feature store (only once)
repo_path = Path(os.getcwd()).resolve().parent / "feature_store"
fs = FeatureStore(repo_path=repo_path)

# Example 1: Get online features and then convert to pandas DataFrame
features = fs.get_online_features(
    features=[
        "customer_features:num_transactions_l1w", 
        "customer_features:total_amount_l1w",
        "customer_features:num_vendors_l1w",
    ],
    entity_rows=[{"user_id": 'user_000066'}]
).to_dict()

print(pd.DataFrame(features))


# Example 2: Get online features and then convert to PySpark DataFrame
spark = init_spark("Feast_Demo")
    
entity_pd_df = pd.DataFrame([{"user_id": "user_000066", "event_timestamp":"2025-03-18"}]) # Column event_timestamp is a feature date

# Retrieve historical features using the Pandas DataFrame.
historical_features = fs.get_historical_features(
    entity_df=entity_pd_df,
    features=[
        "customer_features:num_transactions_l1w",
        "customer_features:total_amount_l1w",
        "customer_features:num_vendors_l1w",
    ]
)

# Convert the historical features (which is a Pandas DataFrame) to a Spark DataFrame.
spark_df = spark.createDataFrame(historical_features.to_df())
spark_df.show()