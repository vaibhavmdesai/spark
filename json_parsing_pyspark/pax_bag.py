from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import json

with open("C:\\BigData\\pax-bag-corr.json", 'r') as f:
    data = json.load(f)

d = {}
l = []
d["dcsPassengerId"] = data["dcsPassengerCorrelations"]["correlationDcsPassengerBagsGroup"]["dcsPassengerId"]
for i in data["dcsPassengerCorrelations"]["correlationDcsPassengerBagsGroup"]["bagsGroupIds"]:
    if i in data["dcsPassengerCorrelations"]["correlationDcsPassengerBagsGroup"]["correlatedData"]:
        # print(f"id exists {i}")

        l.append({"bagGroup_id": i,
                  "bagGroup_info": data["dcsPassengerCorrelations"]["correlationDcsPassengerBagsGroup"]["correlatedData"][i]
                  })
d["bagGroup_id_info"] = l

# print(d)
with open("pax-bag.json", "w") as f1:
    json.dump(d, f1)

# f.explode(f.col("dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.dcsPassengerIds")).alias("passid"),
