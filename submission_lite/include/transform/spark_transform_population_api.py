from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, concat_ws, lit
from pathlib import Path
from datetime import datetime
from include.utils.custom_logging import setup_logging, get_logger

# -------------------------
# CONFIG
# -------------------------
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_PATH = str(BASE_DIR / "data" / "raw" / "worldbank" / "population_all.csv")
SILVER_DIR = str(BASE_DIR / "data" / "silver" / "worldbank")
AUDIT_DIR = str(BASE_DIR / "data" / "audit")

# -------------------------
# LOGGING
# -------------------------
setup_logging()
logger = get_logger(__name__)

# -------------------------
# SPARK SESSION
# -------------------------
def create_spark():
    return SparkSession.builder \
        .appName("TransformPopulation") \
        .getOrCreate()


# -------------------------
# TRANSFORM
# -------------------------
def transform_population_spark(spark):

    try:
        logger.info("Début transformation population (Spark)")

        df = spark.read.parquet(RAW_PATH)

        df_clean = (
            df.dropDuplicates()
              .filter(col("population") > 0)
              .withColumn("year", col("year").cast("int"))
              .withColumn("population", col("population").cast("double"))
              .withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("year_month", concat_ws("-", col("year"), lit("01")))
        )

        logger.info(f"{df_clean.count()} lignes après transformation")

        return df_clean

    except Exception as e:
        logger.error(f"Erreur transformation Spark : {e}")
        return None


# -------------------------
# SAVE + VERSIONING + AUDIT
# -------------------------
def save_population_spark(spark, df):

    try:
        if df is None:
            logger.warning("Aucune donnée à sauvegarder")
            return

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

        output_path = f"{SILVER_DIR}/population_silver_{timestamp}"

        df.write.mode("overwrite").parquet(output_path)

        # Audit
        audit_data = [{
            "table_name": "population_silver",
            "rows": df.count(),
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "file_path": output_path
        }]

        spark.createDataFrame(audit_data) \
            .write.mode("append") \
            .parquet(AUDIT_DIR + "/audit_population")

        logger.info(f"Données Spark silver sauvegardées : {output_path}")

    except Exception as e:
        logger.error(f"Erreur sauvegarde Spark : {e}")


# -------------------------
# MAIN
# -------------------------
def main():
    spark = create_spark()
    df = transform_population_spark(spark)
    save_population_spark(spark, df)
    logger.info("Transformation Spark terminée")
    spark.stop()


if __name__ == "__main__":
    main()