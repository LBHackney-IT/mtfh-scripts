import json
import sys

data = json.load(sys.stdin)
hits = data["hits"]["hits"]
for hit in hits:
    end_date = hit["_source"].get("endOfTenureDate") and hit["_source"]["endOfTenureDate"].split("T")[0]
    print("\n == Tenure ID: " + hit["_source"]["id"] + " == ")
    print("Address: " + hit["_source"]["tenuredAsset"]["fullAddress"])
    print("Start of tenure: " + hit["_source"]["startOfTenureDate"].split("T")[0])
    print("End of tenure: " + (end_date if end_date else "None"))
