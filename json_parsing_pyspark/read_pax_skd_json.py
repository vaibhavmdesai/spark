from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

schema = StructType([StructField("dcsPassengerId",StringType()),StructField("skd_id_info",ArrayType(StructType([StructField("skd_id",StringType()),StructField("skd_info",ArrayType(StructType([StructField("correlatedLegs",ArrayType(StructType([StructField("dcsPassengerLegDeliveryId",StringType()),StructField("scheduleLegId",StringType())]))),StructField("dcsPassengerSegmentDeliveryId",StringType()),StructField("scheduleSegmentId",StringType())])))])))])

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .schema(schema) \
    .load("./pax-skd.json")

df.selectExpr("dcsPassengerId", "explode(skd_id_info) as info") \
    .selectExpr("dcsPassengerId", "info.skd_id as scheduleId", "explode(info.skd_info) as skd_info") \
    .selectExpr("*", "skd_info.dcsPassengerSegmentDeliveryId", "skd_info.scheduleSegmentId"
                , "explode(skd_info.correlatedLegs) as correlatedLegs") \
    .selectExpr("*", "correlatedLegs.*").drop("correlatedLegs", "skd_info") \
    .show(20, False)

