from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

schema = StructType([
    StructField("dcsPassengerId", StringType()),
    StructField("pnr_id_info", ArrayType(
        StructType([
            StructField("pnr_id", StringType()),
            StructField("pnr_info", ArrayType(
                StructType([
                    StructField("dcsPassengerSegmentDeliveryId", StringType()),
                    StructField("pnrAirSegmentId", StringType()),
                    StructField("pnrTravelerId", StringType())
                ])
            ))
        ])
    ))
])

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .schema(schema) \
    .load("./pax.json")

print(df.schema)

# df.show(10, False)

df.selectExpr("dcsPassengerId", "explode(pnr_id_info) as info").selectExpr(
    "dcsPassengerId", "info.pnr_id", "explode(info.pnr_info) as pnr_info"
).selectExpr(
    "*", "pnr_info.*"
).drop(
    "pnr_info"
).show(20, False)

# df.show(10, False)
