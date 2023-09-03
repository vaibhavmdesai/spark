from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()

df = spark.read.format("json") \
    .option("multiLine", "true") \
    .option("recursiveFileLookup", "true") \
    .load("./all-files/*/*.json")

# df.printSchema()

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
                        , "explode(processedBags) as bags") \
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
