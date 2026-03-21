import pytest
from pyspark.sql import SparkSession
import os
import sys

# Configure PySpark to use the current virtual environment's Python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

@pytest.fixture(scope="session")
def spark():
    # Set driver host to localhost to prevent bind connection issues on Windows
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .getOrCreate()

@pytest.mark.skipif(sys.platform == 'win32', reason="PySpark local worker crashes on Windows with Python 3.12+ (WinError 10038)")
def test_spark_session(spark):
    assert spark is not None
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "val"])
    assert df.count() == 2
