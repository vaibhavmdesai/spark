from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

schema = StructType([StructField("dcsPassengerId", StringType()),
                     StructField("bagGroup_id_info", ArrayType(StructType(
    [StructField("bagInventory_id", StringType()),
     StructField("bagInventory_info", ArrayType(StructType([StructField(
        "correlatedLegs", ArrayType(StructType(
            [StructField("dcsPassengerLegDeliveryId", StringType()),
             StructField("inventoryLegId", StringType())]))),
        StructField(
            "dcsPassengerSegmentDeliveryId",
            StringType()),
        StructField(
            "inventoryPartnershipFlightId",
            StringType()),
        StructField(
            "inventorySegmentId",
            StringType())])))])))])

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .schema(schema) \
    .load("./pax-inv.json")

# df.selectExpr("*").show(20, False)

df.selectExpr("dcsPassengerId", "explode(bagGroup_id_info) as info") \
    .selectExpr("dcsPassengerId"
                , "info.bagInventory_id as inventoryIds"
                , "explode(info.bagInventory_info) as bagInventory_info") \
    .selectExpr("*"
                , "bagInventory_info.dcsPassengerSegmentDeliveryId"
                , "bagInventory_info.inventorySegmentId"
                , "bagInventory_info.inventoryPartnershipFlightId"
                , "explode(bagInventory_info.correlatedLegs) as correlatedLegs") \
    .selectExpr("*", "correlatedLegs.*") \
    .drop("correlatedLegs", "bagInventory_info") \
    .show(20, False)