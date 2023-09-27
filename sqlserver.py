import pyspark
from pyspark.sql import SparkSession
import os

scala_version = '2.12'
spark_version = pyspark.__version__

def configure_spark():
    existing_spark = SparkSession.builder.getOrCreate()
    if existing_spark:
        return existing_spark

    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        'org.apache.kafka:kafka-clients:3.5.1'
    ]

    os.environ.setdefault('PYSPARK_SUBMIT_ARGS', f'--packages {",".join(packages)} pyspark-shell')

    spark = SparkSession.builder \
        .appName("SQLServerToKafka") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g").getOrCreate()

    return spark

def read_data_from_sql_server(spark, jdbc_url, user, password, table_name):
    properties = {
        "user": user,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "encrypt": "false"
    }

    sql_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties, numPartitions=5)
    sql_df.cache()

    return sql_df
