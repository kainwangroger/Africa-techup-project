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
from delta import configure_spark_with_delta_pip

def create_spark_session(app_name="BooksTransform"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


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
    # Data Validation (Basic)
    # -----------------------------
    total_count = df_clean.count()
    null_titles = df_clean.filter(col("titre").isNull()).count()
    if null_titles > 0:
        logger.warning(f"⚠️ {null_titles} livres ont des titres nuls sur {total_count}")

    # -----------------------------
    # Création dossier de sortie
    # -----------------------------
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # -----------------------------
    # Écriture Delta Lake
    # -----------------------------
    output_delta = str(output_path).replace(".csv", "_delta")
    
    # Workaround for Docker on Windows: chmod fails on mounted volumes 
    # when Spark tries to create the _delta_log directory. 
    # We write to /tmp first and then copy without preserving permissions.
    import shutil
    import uuid
    
    tmp_delta_path = f"/tmp/delta_{uuid.uuid4().hex}"
    df_clean.write.format("delta").mode("overwrite").save(tmp_delta_path)
    
    def copy_no_stats(src, dst):
        os.makedirs(dst, exist_ok=True)
        for item in os.listdir(src):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            if os.path.isdir(s):
                copy_no_stats(s, d)
            else:
                shutil.copyfile(s, d)

    if os.path.exists(output_delta):
        shutil.rmtree(output_delta)
        
    copy_no_stats(tmp_delta_path, output_delta)
    shutil.rmtree(tmp_delta_path)

    logger.info(f"Transformation PySpark terminée | Format: Delta | Path: {output_delta}")
    print(f"✅ Données transformées sauvegardées au format Delta dans {output_delta}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    transform_books_spark()

