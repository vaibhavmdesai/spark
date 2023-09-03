from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

# schema1 = StructType([
# StructField("bagsGroupId", StringType()),
# StructField("pass_id_info", ArrayType(StructType([
# StructField("pass_id", StringType()),
# StructField("pass_info", ArrayType(StructType([
# StructField("bagId", StringType()),
# StructField("bagLegCorrelations", ArrayType(StructType([
# StructField("bagLegDeliveryId", StringType()),
# StructField("dcsPassengerLegDeliveryId", StringType()),
# StructField("dcsPassengerSegmentDeliveryId", StringType())
# ])))
# ])))
# ])))
# ])

schema2 = StructType([
StructField("bagsGroupId", StringType()),
StructField("pnr_id_info", ArrayType(StructType([
StructField("pnr_id", StringType()),
StructField("pnr_info", ArrayType(StructType([
StructField("bagId", StringType()),
StructField("pnrTravelerId", StringType()),
StructField("bagLegCorrelations", ArrayType(StructType([
StructField("bagLegDeliveryId", StringType()),
StructField("pnrAirSegmentId", StringType())
])))
])))
])))
])

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .schema(schema2) \
    .load("./abc6.json")

# df.show(10, False)
#
# df.selectExpr("bagsGroupId", "explode(pass_id_info) as info").selectExpr(
#     "bagsGroupId", "info.pass_id", "explode(info.pass_info) as pass_info"
# ).selectExpr(
#     "*", "pass_info.*"
# ).drop(
#     "pass_info"
# ).selectExpr(
#     "*", "explode(bagLegCorrelations) as bagLegCorrelations1"
# ).drop(
#     "bagLegCorrelations"
# ).select(
#     "*", "bagLegCorrelations1.*"
# ).drop(
#     "bagLegCorrelations1"
# ).show(20, False)

df.show(10, False)