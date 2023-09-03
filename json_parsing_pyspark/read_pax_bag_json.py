from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

schema = StructType([StructField("dcsPassengerId", StringType()), StructField("bagGroup_id_info", ArrayType(StructType(
    [StructField("bagGroup_id", StringType()), StructField("bagGroup_info", ArrayType(StructType([StructField(
        "correlatedLegs", ArrayType(StructType(
            [StructField("bagId", StringType()), StructField("bagLegDeliveryId", StringType()),
             StructField("dcsPassengerLegDeliveryId", StringType())]))), StructField("dcsPassengerSegmentDeliveryId",
                                                                                     StringType())])))])))])

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .schema(schema) \
    .load("./pax-bag.json")

df.selectExpr("dcsPassengerId", "explode(bagGroup_id_info) as info") \
    .selectExpr("dcsPassengerId", "info.bagGroup_id as bagGroupId", "explode(info.bagGroup_info) as bagGroup_info") \
    .selectExpr("*", "bagGroup_info.dcsPassengerSegmentDeliveryId"
                , "explode(bagGroup_info.correlatedLegs) as correlatedLegs") \
    .selectExpr("*", "correlatedLegs.*").drop("correlatedLegs", "bagGroup_info") \
    .show(20, False)
