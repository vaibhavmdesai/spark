from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import json

with open("C:\\BigData\\PassengerCorrelationDatapushExample.json", 'r') as f:
    data = json.load(f)

d = {}
l = []
d["dcsPassengerId"] = data["dcsPassengerCorrelations"]["correlationDcsPassengerTicket"]["dcsPassengerId"]
for i in data["dcsPassengerCorrelations"]["correlationDcsPassengerTicket"]["ticketIds"]:
    if i in data["dcsPassengerCorrelations"]["correlationDcsPassengerTicket"]["correlatedData"]:
        # print(f"id exists {i}")

        l.append({"ticket_id": i,
                  "ticket_info": data["dcsPassengerCorrelations"]["correlationDcsPassengerTicket"]["correlatedData"][i]
                  })
d["ticket_id_info"] = l

# print(d)
with open("pax-tkt.json", "w") as f1:
    json.dump(d, f1)

# f.explode(f.col("dcsBaggageCorrelations.correlationBagsGroupDcsPassenger.dcsPassengerIds")).alias("passid"),
