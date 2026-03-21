from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def transform_wine_spark(input_path: str,
                         output_path: str):

    spark = SparkSession.builder \
        .appName("Wine Silver Transformation") \
        .getOrCreate()

    df = spark.read.parquet(input_path)

    df_clean = (
        df
        .dropDuplicates(["title"])
        .filter(col("country").isNotNull())
        .filter(col("price").isNotNull())
        .withColumn("price", col("price").cast("double"))
        .withColumn("points", col("points").cast("integer"))
        .withColumn(
            "price_per_point",
            col("price") / col("points")
        )
        .withColumn("processed_at", current_timestamp())
    )

    # Partition stratégique
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("country") \
        .parquet(output_path)

    spark.stop()