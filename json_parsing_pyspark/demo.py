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
    recs = json.load(f)
# print(type(data))
# data = json.loads(res)

# print(recs)

# for data in recs:
#     d = {}
#     l = []
#     d["bagsGroupId"] = data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["bagsGroupId"]
#     for i in data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["dcsPassengerIds"]:
#         if i in data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["correlatedData"]:
#             # print(f"id exists {i}")
#
#             l.append({"pass_id": i,
#                       "pass_info": data["dcsBaggageCorrelations"]["correlationBagsGroupDcsPassenger"]["correlatedData"][i]
#                       })
#     d["pass_id_info"] = l
#
#     # print(d)
#     with open("abc5.json", "a") as f1:
#         json.dump(d, f1)
#         f1.write(",")
#         f1.write("\n")


for data in recs:
    d = {}
    l = []
    d["bagsGroupId"] = data["dcsBaggageCorrelations"]["correlationBagsGroupPnr"]["bagsGroupId"]
    for i in data["dcsBaggageCorrelations"]["correlationBagsGroupPnr"]["pnrIds"]:
        if i in data["dcsBaggageCorrelations"]["correlationBagsGroupPnr"]["correlatedData"]:
            # print(f"id exists {i}")

            l.append({"pnr_id": i,
                      "pnr_info": data["dcsBaggageCorrelations"]["correlationBagsGroupPnr"]["correlatedData"][i]
                      })
    d["pnr_id_info"] = l

    # print(d)
    with open("abc6.json", "a") as f1:
        json.dump(d, f1)
        f1.write(",")
        f1.write("\n")