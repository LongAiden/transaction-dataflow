import pytest
from scripts.utils import init_spark
from pyspark.sql import functions as F

@pytest.fixture(scope="module")
def spark():
    # Initialize a local SparkSession (uses your existing ./scripts/.env)
    spark = init_spark("test_app", local_mode=True)
    yield spark
    spark.stop()

def test_init_spark_local_mode(spark):
    """
    Verify that init_spark in local mode returns a SparkSession
    running in local[*] with the correct application name.
    """
    master = spark.sparkContext.master
    assert master.startswith("local"), f"Expected local master, got {master}"
    assert spark.sparkContext.appName == "test_app"

def test_feature_aggregation_columns(spark):
    """
    Build a tiny DataFrame and apply the same l1w aggregation logic
    to confirm the output schema has exactly the nine expected columns.
    """
    # sample data: (User ID, Transaction ID, Amount, Vendor, Sources, Time)
    data = [
        ("userA", "tx1", 10.0, "V1", "S1", "2025-03-05 00:00:00"),
        ("userA", "tx2", 20.0, "V2", "S2", "2025-03-06 00:00:00"),
        ("userB", "tx3",  5.0, "V1", "S1", "2025-03-07 00:00:00"),
    ]
    df = spark.createDataFrame(
        data,
        schema=["User ID","Transaction ID","Amount","Vendor","Sources","Time"]
    )

    time_window = "l1w"
    agg = (
        df.groupBy("User ID")
          .agg(
              F.count("Transaction ID").alias(f"num_transactions_{time_window}"),
              F.sum("Amount").alias(f"total_amount_{time_window}"),
              F.avg("Amount").alias(f"avg_amount_{time_window}"),
              F.min("Amount").alias(f"min_amount_{time_window}"),
              F.max("Amount").alias(f"max_amount_{time_window}"),
              F.countDistinct("Vendor").alias(f"num_vendors_{time_window}"),
              F.countDistinct("Sources").alias(f"num_sources_{time_window}")
          )
          .withColumnRenamed("User ID", "user_id")
          .withColumn("date", F.lit("2025-03-10"))
    )

    expected = [
        "user_id",
        "num_transactions_l1w",
        "total_amount_l1w",
        "avg_amount_l1w",
        "min_amount_l1w",
        "max_amount_l1w",
        "num_vendors_l1w",
        "num_sources_l1w",
        "date",
    ]
    # Ensure no extra or missing columns
    assert set(agg.columns) == set(expected)