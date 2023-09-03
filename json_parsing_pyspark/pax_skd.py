from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import json

with open("C:\\BigData\\pax-skd-corr.json", 'r') as f:
    data = json.load(f)

d = {}
l = []
d["dcsPassengerId"] = data["dcsPassengerCorrelations"]["correlationDcsPassengerSchedule"]["dcsPassengerId"]
for i in data["dcsPassengerCorrelations"]["correlationDcsPassengerSchedule"]["scheduleIds"]:
    if i in data["dcsPassengerCorrelations"]["correlationDcsPassengerSchedule"]["correlatedData"]:
        # print(f"id exists {i}")

        l.append({"skd_id": i,
                  "skd_info": data["dcsPassengerCorrelations"]["correlationDcsPassengerSchedule"]["correlatedData"][i]
                  })
d["skd_id_info"] = l

# print(d)
with open("pax-skd.json", "w") as f1:
    json.dump(d, f1)

# f.explode(f.col("dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.dcsPassengerIds")).alias("passid"),
