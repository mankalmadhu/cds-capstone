from pyspark.sql import SparkSession
from delta import *
from functools import cache


@cache
def build():
    builder = SparkSession.builder.appName("Tweet_Analysis") \
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return configure_spark_with_delta_pip(builder).getOrCreate()
