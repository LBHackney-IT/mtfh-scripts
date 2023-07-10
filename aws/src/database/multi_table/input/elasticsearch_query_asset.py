import json
import sys

data = json.load(sys.stdin)
hits = data["hits"]["hits"]
for hit in hits:
    source = hit["_source"]
    end_date = source["tenure"].get("endOfTenureDate") and source["tenure"]["endOfTenureDate"].split("T")[0]
    print("\n == Asset ID: " + source["id"] + " == ")
    print("Address: " + source["assetAddress"]["addressLine1"])
    print("Start of tenure: " + source["tenure"]["startOfTenureDate"].split("T")[0])
    print("End of tenure: " + (end_date if end_date else "None"))
