from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def transform_countries_spark(input_path: str,
                               output_path: str):

    spark = SparkSession.builder \
        .appName("Countries Silver Transformation") \
        .getOrCreate()

    df = spark.read.parquet(input_path)

    # -------------------------
    # Nettoyage
    # -------------------------
    df_clean = (
        df
        .dropDuplicates(["iso3"])
        .filter(col("iso3").isNotNull())
        .withColumn("population", col("population").cast("long"))
        .withColumn("area", col("area").cast("double"))
        .withColumn(
            "population_density",
            col("population") / col("area")
        )
        .withColumn("processed_at", current_timestamp())
    )

    # -------------------------
    # Écriture Silver
    # -------------------------
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("region") \
        .parquet(output_path)

    spark.stop()