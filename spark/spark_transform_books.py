from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_replace,
    regexp_extract,
    when
)
import logging
import os


# --------------------------------------------------
# LOGGING
# --------------------------------------------------
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


# --------------------------------------------------
# SPARK
# --------------------------------------------------
def create_spark_session(app_name="BooksTransform"):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


# --------------------------------------------------
# TRANSFORM
# --------------------------------------------------
def transform_books_spark(
    input_path="data/raw/books_raw_part_*.csv",
    output_path="data/processed/books_clean"
):
    setup_logging()
    logging.info("Démarrage transformation PySpark")

    spark = create_spark_session()

    # Lecture de tous les CSV partiels
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    # Nettoyage des prix
    df = df.withColumn(
        "price_excl_tax",
        regexp_replace(col("prix_hors_taxe"), "£", "").cast("double")
    ).withColumn(
        "price_incl_tax",
        regexp_replace(col("prix_ttc"), "£", "").cast("double")
    )

    # Nettoyage stock : In stock (22 available) -> 22
    df = df.withColumn(
        "stock",
        regexp_extract(col("disponibilite"), r"\((\d+)", 1).cast("int")
    )

    # Reviews
    df = df.withColumn(
        "reviews",
        when(col("nombre_davis").isNull(), 0)
        .otherwise(col("nombre_davis").cast("int"))
    )

    # Sélection finale
    df_clean = df.select(
        "url",
        "titre",
        "description",
        "upc",
        "type_produit",
        "price_excl_tax",
        "price_incl_tax",
        "stock",
        "reviews"
    )

    # Création dossier si besoin
    os.makedirs(output_path, exist_ok=True)

    # Écriture parquet (recommandé en Spark)
    df_clean.write.mode("overwrite").parquet(output_path)

    logging.info("Transformation PySpark terminée")
    print(f"✅ Données transformées sauvegardées dans {output_path}")

    spark.stop()


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    transform_books_spark()
