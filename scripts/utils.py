from pyspark.sql import SparkSession

def init_spark(spark_app_name:str):
    print("Initializing Spark session...")
    spark = (SparkSession.builder
        .appName(spark_app_name)
        .config("spark.master", "local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        
        # Delta Lake config
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0,org.apache.hadoop:hadoop-aws:3.3.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        # S3 configuration for local MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        
        # For better performance
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.fallback.enabled", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .getOrCreate())
    
    print("Spark session initialized")
    return spark