from pyspark.sql import SparkSession
import os

def get_spark_session(app_name="IBEX35"):
    # Ruta relativa al proyecto
    jar_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../lib/mysql-connector-j-9.2.0.jar"))

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jar_path) \
        .getOrCreate()
    return spark

def cargar_csv(spark, path="ibex35_close-2024.csv"):
    df = spark.read.option("header", True) \
                   .option("sep", ";") \
                   .option("inferSchema", True) \
                   .csv(path)
    return df
