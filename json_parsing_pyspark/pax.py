from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import json

# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#
# df = spark.read.format("json") \
#     .option("multiLine", "true") \
#     .load("file:///C:/BigData/test.json")
#
# # df.show()
# df.selectExpr("dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.bagsGroupId as bagsGroupId",
#               "dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.dcsPassengerIds",
#               "dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.correlatedData")

with open("C:\\BigData\\pax-corr.json", 'r') as f:
    data = json.load(f)
# print(type(data))
# data = json.loads(res)

# print(data)

d = {}
l = []
d["dcsPassengerId"] = data["dcsPassengerCorrelations"]["correlationDcsPassengerPnr"]["dcsPassengerId"]
for i in data["dcsPassengerCorrelations"]["correlationDcsPassengerPnr"]["pnrIds"]:
    if i in data["dcsPassengerCorrelations"]["correlationDcsPassengerPnr"]["correlatedData"]:
        # print(f"id exists {i}")

        l.append({"pnr_id": i,
                  "pnr_info": data["dcsPassengerCorrelations"]["correlationDcsPassengerPnr"]["correlatedData"][i]
                  })
d["pnr_id_info"] = l

# print(d)
with open("pax.json", "w") as f1:
    json.dump(d,f1)

# f.explode(f.col("dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.dcsPassengerIds")).alias("passid"),
