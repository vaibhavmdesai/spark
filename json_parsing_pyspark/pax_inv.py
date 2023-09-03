from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import json

with open("C:\\BigData\\PassengerCorrelationDatapushExample.json", 'r') as f:
    data = json.load(f)

d = {}
l = []
d["dcsPassengerId"] = data["dcsPassengerCorrelations"]["correlationDcsPassengerInventory"]["dcsPassengerId"]
for i in data["dcsPassengerCorrelations"]["correlationDcsPassengerInventory"]["inventoryIds"]:
    if i in data["dcsPassengerCorrelations"]["correlationDcsPassengerInventory"]["correlatedData"]:
        # print(f"id exists {i}")

        l.append({"bagInventory_id": i,
                  "bagInventory_info": data["dcsPassengerCorrelations"]["correlationDcsPassengerInventory"]["correlatedData"][i]
                  })
d["bagGroup_id_info"] = l

# print(d)
with open("pax-inv.json", "w") as f1:
    json.dump(d, f1)

# f.explode(f.col("dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.dcsPassengerIds")).alias("passid"),
