import pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_replace,
    regexp_extract,
    when
)
# from utils.custom_logging import setup_logging, get_logger
import os
import logging


# --------------------------------------------------
# LOGGING
# --------------------------------------------------
# setup_logging()
# logger = get_logger(__name__)
logger = logging.getLogger(__name__)


# -------------------------------------------------
# PATHS
# -------------------------------------------------

BATCH_SIZE = 100
TOTAL_BOOKS = 1000

BASE_DIR = pathlib.Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LINKS_FILE = DATA_DIR / "raw" / "book_links.txt"
CSV_FILES = DATA_DIR / "raw" / "books_raw_unique.csv"
JSON_FILES = DATA_DIR / "raw" / "books_raw_unique.json"

CSV_BATCH_FILES = DATA_DIR / "raw" / "books_raw_batch_*.csv"

OUTPUT_FILES = DATA_DIR / "processed" / "books_clean.csv"


# --------------------------------------------------
# SPARK
# --------------------------------------------------
def create_spark_session(app_name="BooksTransform"):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def transform_books_spark(
    input_path=CSV_FILES,
    output_path=OUTPUT_FILES
):
    logger.info("Démarrage transformation PySpark")

    spark = create_spark_session()

    # Lecture CSV
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(input_path))
    )

    # -----------------------------
    # Nettoyage des prix
    # -----------------------------
    for col_name in ["prix_hors_taxe", "prix_ttc"]:
        df = df.withColumn(
            f"{col_name}_clean",
            regexp_replace(col(col_name), "[^0-9.]", "")
        ).withColumn(
            f"{col_name}_clean",
            when(col(f"{col_name}_clean").isin("", "."), None)
            .otherwise(col(f"{col_name}_clean"))
        ).withColumn(
            f"{'price_excl_tax' if col_name=='prix_hors_taxe' else 'price_incl_tax'}", # noqa E501
            col(f"{col_name}_clean").cast("double")
        ).drop(f"{col_name}_clean")

    # -----------------------------
    # Nettoyage stock
    # -----------------------------
    df = df.withColumn(
        "stock",
        regexp_extract(col("disponibilite"), r"\((\d+)", 1).cast("int")
    )

    # -----------------------------
    # Nettoyage reviews
    # -----------------------------
    df = df.withColumn(
        "reviews",
        when(col("nombre_davis").isNull(), 0)
        .otherwise(col("nombre_davis").cast("int"))
    )

    # -----------------------------
    # Sélection finale des colonnes
    # -----------------------------
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

    # -----------------------------
    # Création dossier de sortie
    # -----------------------------
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # -----------------------------
    # Écriture Parquet
    # -----------------------------
    df_clean.write.mode("overwrite").parquet(str(output_path), compression="snappy") # noqa E501

    logger.info("Transformation PySpark terminée")
    print(f"✅ Données transformées sauvegardées dans {output_path}")

    spark.stop()


# import pathlib
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     regexp_replace,
#     regexp_extract,
#     when
# )
# # from utils.custom_logging import setup_logging, get_logger
# import os
# import logging

# # --------------------------------------------------
# # LOGGING
# # --------------------------------------------------
# # setup_logging()
# # logger = get_logger(__name__)
# logger = logging.getLogger(__name__)


# # -------------------------------------------------
# # PATHS
# # -------------------------------------------------

# BATCH_SIZE = 100
# TOTAL_BOOKS = 1000

# BASE_DIR = pathlib.Path(__file__).resolve().parents[2]
# DATA_DIR = BASE_DIR / "data"
# DATA_DIR.mkdir(parents=True, exist_ok=True)

# LINKS_FILE = DATA_DIR / "raw" / "book_links.txt"
# CSV_FILES = DATA_DIR / "raw" / "books_raw_unique.csv"
# JSON_FILES = DATA_DIR / "raw" / "books_raw_unique.json"

# CSV_BATCH_FILES = DATA_DIR / "raw" / "books_raw_batch_*.csv"

# OUTPUT_FILES = DATA_DIR / "processed" / "books_clean.csv"


# # --------------------------------------------------
# # SPARK
# # --------------------------------------------------
# def create_spark_session(app_name="BooksTransform"):
#     return (
#         SparkSession.builder
#         .appName(app_name)
#         .getOrCreate()
#     )


# # --------------------------------------------------
# # TRANSFORM
# # --------------------------------------------------
# def transform_books_spark(
#     input_path=CSV_FILES,
#     output_path=OUTPUT_FILES
# ):
#     logger.info("Démarrage transformation PySpark")

#     spark = create_spark_session()

#     # Lecture de tous les CSV partiels
#     df = (
#         spark.read
#         .option("header", True)
#         .option("inferSchema", True)
#         .csv(str(input_path))
#     )

#     # Nettoyage des prix
#     df = df.withColumn(
#         "price_excl_tax",
#         # regexp_replace(col("prix_hors_taxe"), "£", "").cast("double")
#         regexp_replace(col("prix_hors_taxe"), "[^0-9.]", "").cast("double")
#     ).withColumn(
#         "price_incl_tax",
#         # regexp_replace(col("prix_ttc"), "£", "").cast("double")
#         regexp_replace(col("prix_ttc"), "[^0-9.]", "").cast("double")

#     )

#     # Nettoyage stock : In stock (22 available) -> 22
#     df = df.withColumn(
#         "stock",
#         regexp_extract(col("disponibilite"), r"\((\d+)", 1).cast("int")
#     )

#     # Reviews
#     df = df.withColumn(
#         "reviews",
#         when(col("nombre_davis").isNull(), 0)
#         .otherwise(col("nombre_davis").cast("int"))
#     )

#     # Sélection finale
#     df_clean = df.select(
#         "url",
#         "titre",
#         "description",
#         "upc",
#         "type_produit",
#         "price_excl_tax",
#         "price_incl_tax",
#         "stock",
#         "reviews"
#     )

#     # Création dossier si besoin
#     # os.makedirs(output_path, exist_ok=True)

#     os.makedirs(os.path.dirname(output_path), exist_ok=True)
#     # output_path.parent.mkdir(parents=True, exist_ok=True)

#     # Écriture parquet (recommandé en Spark)
#     df_clean.write.mode("overwrite").parquet(str(output_path))

#     logger.info("Transformation PySpark terminée")
#     print(f"✅ Données transformées sauvegardées dans {output_path}")

#     spark.stop()


# # --------------------------------------------------
# # MAIN
# # --------------------------------------------------
# if __name__ == "__main__":
#     transform_books_spark()
