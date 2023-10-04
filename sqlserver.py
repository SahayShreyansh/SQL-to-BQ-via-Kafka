import pyspark
from pyspark.sql import SparkSession
import os

scala_version = '2.12'
spark_version = pyspark.__version__
sql_driver_jar = "C:/Users/Admin/Downloads/sqljdbc_12.4.1.0_enu/sqljdbc_12.4/enu/jars/mssql-jdbc-12.4.0.jre11.jar"

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
        .config("spark.executor.memory", "4g")\
        .config("spark.driver.extraClassPath", sql_driver_jar) .config("spark.executor.extraClassPath", sql_driver_jar).getOrCreate()

    return spark

def read_data_from_sql_server(spark, jdbc_url, user, password, table_name):
    properties = {
        "user": user,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "encrypt": "false",
        "integratedSecurity":"True"
    }

    sql_df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=properties, numPartitions=5)
    sql_df.cache()

    return sql_df
