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

with open("C:\\BigData\\test.json", 'r') as f:
    data = json.load(f)
# print(type(data))
# data = json.loads(res)

print(data)

# d = {}
# l = []
# d["bagsGroupId"] = data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["bagsGroupId"]
# for i in data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["dcsPassengerIds"]:
#     if i in data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["correlatedData"]:
#         # print(f"id exists {i}")
#
#         l.append({"pass_id": i,
#                   "pass_info": data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["correlatedData"][i]})
#         # d["dcsPassengerIds"] = i
#         # d["passengerInfo"] = data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["correlatedData"][i]
# d["pass_id_info"] = l
#
# # print(d)
# with open("abc2.json", "w") as f1:
#     f1.write(json.dumps(d))

# f.explode(f.col("dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.dcsPassengerIds")).alias("passid"),
