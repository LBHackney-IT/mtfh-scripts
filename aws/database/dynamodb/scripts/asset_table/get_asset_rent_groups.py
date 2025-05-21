"""
A script to retrieve assetId and rentGroup from a DynamoDB table and export it to a CSV file.
The script scans the DynamoDB table, retrieves the specified attributes, and handles pagination if necessary.
"""

import boto3
import csv
from mypy_boto3_dynamodb.service_resource import Table

session = boto3.Session(profile_name="housingdev")

# Initialize DynamoDB resource
dynamodb = session.resource("dynamodb")
table: Table = dynamodb.Table("Assets")

# Scan the table - specifying the attributes to retrieve
response = table.scan(ProjectionExpression="assetId, rentGroup")
items = response["Items"]

# Handle pagination (if there are more than 1MB of items)
while "LastEvaluatedKey" in response:
    response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
    items.extend(response["Items"])

# Extract assetId and rentGroup (default rentGroup to empty string if missing)
filtered_data = [
    {"assetId": item["assetId"], "rentGroup": item.get("rentGroup", "")}
    for item in items
    if item.get("assetId") is not None
]

# Write the filtered data to CSV (tab-separated)
with open("assets_export.csv", mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=["assetId", "rentGroup"], delimiter="\t")
    writer.writeheader()
    writer.writerows(filtered_data)

print("CSV file created: assets_export.csv")
