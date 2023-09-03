# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

# schema = StructType([StructField("processedBags",ArrayType(StructType([StructField("bagTag",StructType([StructField("finalDestination",StringType(),True),StructField("issuingCarrier",StringType(),True),StructField("licensePlate",StringType(),True),StructField("systemSource",StringType(),True)]),True),StructField("bagType",StringType(),True),StructField("bagsGroupId",ArrayType(StructType([StructField("id",StringType(),True)]),True),True),StructField("id",StringType(),True),StructField("legs",ArrayType(StructType([StructField("acceptance_status",StringType(),True),StructField("activation_status",StringType(),True),StructField("authorityToLoad",BooleanType(),True),StructField("authorityToTransport",BooleanType(),True),StructField("boardPoint",StructType([StructField("iataCode",StringType(),True),StructField("localDateTime",StringType(),True)]),True),StructField("checkinLocation",StructType([StructField("trackedLocation",StructType([StructField("identifier",StringType(),True)]),True)]),True),StructField("flightSegmentRefs",ArrayType(StructType([StructField("id",StringType(),True)]),True),True),StructField("id",StringType(),True),StructField("offPoint",StructType([StructField("iataCode",StringType(),True)]),True),StructField("operatingFlight",StructType([StructField("carrierCode",StringType(),True),StructField("flightNumber",StringType(),True),StructField("operationalSuffix",StringType(),True)]),True),StructField("travelClassCode",StringType(),True)]),True),True),StructField("owner",StructType([StructField("id",StringType(),True),StructField("type",StringType(),True)]),True),StructField("systemSource",StringType(),True),StructField("weight",StructType([StructField("unit",StringType(),True),StructField("value",LongType(),True)]),True)]),True),True),StructField("processedBagsGroup",StructType([StructField("checkedBags",StructType([StructField("collection",ArrayType(StructType([StructField("id",StringType(),True),StructField("type",StringType(),True)]),True),True)]),True),StructField("checkedBagsCount",LongType(),True),StructField("checkedBagsWeight",StructType([StructField("unit",StringType(),True),StructField("value",LongType(),True)]),True),StructField("id",StringType(),True),StructField("itinerary",ArrayType(StructType([StructField("boardPoint",StructType([StructField("iataCode",StringType(),True)]),True),StructField("id",StringType(),True),StructField("offPoint",StructType([StructField("iataCode",StringType(),True)]),True)]),True),True),StructField("pooledPassengers",StructType([StructField("collection",ArrayType(StructType([StructField("id",StringType(),True),StructField("type",StringType(),True)]),True),True)]),True),StructField("responsiblePassenger",StructType([StructField("id",StringType(),True),StructField("type",StringType(),True)]),True)]),True),StructField("id",StringType(),True),StructField("version",StringType(),True)])


# COMMAND ----------

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .load("./bag-new.json")

# df.show(20, False)
# df.printSchema()

# COMMAND ----------

bagsGroup_DF = df.selectExpr("processedBagsGroup.responsiblePassenger.id as responsiblePassengerId"
                             , "cast('2023-03-27' as date) as lastmodificationdatetime"
                             , "processedBagsGroup.checkedBagsCount"
                             , "1679906580 as etag"
                             , "cast(null as int) as handBagsCount"
                             , "processedBagsGroup.id as processedBagsGroupId"
                             , "cast(null as string) as type"
                             , "cast('2023-04-04' as date) as processed_date")

# bagsGroup_DF.show(10, False)

# COMMAND ----------

bags_DF = df.selectExpr("processedBagsGroup.id as processedBagsGroupid"
                        ,"explode(processedBags) as bags") \
    .selectExpr("*"
                , "cast('2023-03-27' as date) as lastmodificationdatetime"
                , "bags.id as processedBagsid"
                , "bags.weight.value as bagWeight"
                , "bags.weight.unit as bagWeightUnit"
                , "bags.bagType"
                , "bags.systemSource"
                , "bags.owner.id as ownerId"
                , "cast('2023-04-04' as date) as processed_date") \
    .drop("bags")


# bags_DF.show(10, False)

# COMMAND ----------

bagsGroupWeight_DF = df.selectExpr("processedBagsGroup.id as processedBagsGroupId"
                                   , "processedBagsGroup.checkedBagsCount"
                                   , "processedBagsGroup.checkedBagsWeight.unit as weightUnit"
                                   , "processedBagsGroup.checkedBagsWeight.value as weightValue"
                                   , "cast('2023-03-27' as date) as lastmodificationdatetime"
                                   , "cast('2023-04-04' as date) as processed_date")

# bagsGroupWeight_DF.show(10, False)

# COMMAND ----------

bagsTag_DF = df.selectExpr("explode(processedBags) as bags") \
    .selectExpr("cast('2023-03-27' as date) as lastmodificationdatetime"
                , "bags.id as processedBagsid"
                , "bags.bagType"
                , "bags.bagTag.finalDestination"
                , "bags.bagTag.issuingCarrier"
                , "bags.bagTag.licensePlate"
                , "bags.bagTag.systemSource"
                , "cast('2023-04-04' as date) as processed_date")

# bagsTag_DF.show(10, False)

# COMMAND ----------

bagsFlightDesignator_DF = df.selectExpr("explode(processedBags) as bags") \
    .selectExpr("cast('2023-03-27' as date) as lastmodificationdatetime"
                , "bags.id as processedBagsid"
                , "explode(bags.legs) as legs") \
    .selectExpr("*"
                , "legs.id as legsid"
                , "legs.boardPoint.iataCode as boardPoint"
                , "legs.boardPoint.localDateTime as boardDate"
                , "legs.offPoint.iataCode as offPoint"
                , "legs.operatingFlight.carrierCode"
                , "legs.operatingFlight.flightNumber"
                , "cast(null as string) as operationalSuffix"
                , "cast('2023-04-04' as date) as processed_date") \
    .drop("legs")

# bagsFlightDesignator_DF.filter(" processedBagsid == '10AC810000106F71' ").show(10, False)

# COMMAND ----------
