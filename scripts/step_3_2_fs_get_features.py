import os
import pandas as pd
from pyspark.sql import SparkSession
from feast import FeatureStore
from pathlib import Path
from utils import init_spark

# Init Spark session
spark = init_spark("Feast_Demo", local_mode=True) # local_mode=True for local testing

# Initialize feature store
# If you run this script in Airflow container repo_path should be /opt/airflow/feature_store
repo_path = "feature_store" 
fs = FeatureStore(repo_path=repo_path)

# Example 1: Get online features
features = fs.get_online_features(
    features=[
        "customer_features:num_transactions_l1w", 
        "customer_features:total_amount_l1w",
        "customer_features:num_vendors_l1w",
    ],
    entity_rows=[{"user_id": 'user_000066'}]
).to_dict()

print(pd.DataFrame(features))

# Convert the online features to a Spark DataFrame
spark_online_df = spark.createDataFrame(pd.DataFrame(features))
spark_online_df.show()


# Example 2: Get offline features => You need to provide event_timestamp as a feature date
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

print(historical_features.to_df())

# Convert the offline features to a Spark DataFrame
spark_offline_df = spark.createDataFrame(historical_features.to_df())
spark_offline_df.show()