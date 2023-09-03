from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()


df = spark.read\
    .format("com.databricks.spark.xml")\
    .option("rowTag", "errors")\
    .option("rootTag", "DCSCDWFeed_CDWCPRIdentification")\
    .load("./newXML.xml")

df.printSchema()
