from pyspark.sql import SparkSession
from pyspark import SparkConf
from delta import pip_utils
from functools import cache
import os


@cache
def build():
    conf = SparkConf()
    conf.set(
        'spark.jars.packages',
        'io.delta:delta-core_2.12:1.1.0,org.apache.hadoop:hadoop-aws:3.2.2')
    conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
             'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
    conf.set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    conf.set('spark.sql.catalog.spark_catalog',
             'org.apache.spark.sql.delta.catalog.DeltaCatalog')

    builder = SparkSession.builder.appName("Tweet_Analysis")

    spark = pip_utils.configure_spark_with_delta_pip(builder).config(
        conf=conf).getOrCreate()
    sc = spark.sparkContext

    hadoop_config = sc._jsc.hadoopConfiguration()
    hadoop_config.set('fs.s3a.access.key', os.environ['do_storage_key'])
    hadoop_config.set('fs.s3a.secret.key', os.environ['do_storage_secret'])
    hadoop_config.set("fs.s3a.endpoint",
                      "https://sgp1.digitaloceanspaces.com/")

    return spark
