from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import json

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

schema = StructType([StructField("dcsPassengerId", StringType()), StructField("ticket_id_info", ArrayType(StructType(
    [StructField("ticket_id", StringType()), StructField("ticket_info", ArrayType(StructType(
        [StructField("dcsPassengerSegmentDeliveryId", StringType()),
         StructField("ticketCouponId", StringType())])))])))])

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .schema(schema) \
    .load("./pax-tkt.json")

# df.show(20, False)
# df.selectExpr("*").show(20, False)

df.selectExpr("dcsPassengerId", "explode(ticket_id_info) as info") \
    .selectExpr("dcsPassengerId", "info.ticket_id as ticketId"
                , "explode(info.ticket_info) as ticket_info") \
    .selectExpr("*"
                , "ticket_info.dcsPassengerSegmentDeliveryId"
                , "ticket_info.ticketCouponId") \
    .drop("ticket_info") \
    .show(20, False)

