"""
A script to retrieve assetId and rentGroup from the Assets DynamoDB table
And export the results to either:
1)  a CSV file.
2)  a SQL file for importing into a database.
The script scans the DynamoDB table, retrieves the specified attributes, and handles pagination if necessary.
"""

import boto3
import csv
import argparse
from mypy_boto3_dynamodb.service_resource import Table

# File format argument
fileFormat = "sql"  # argument "csv" for CSV file - or "sql" for SQL file

# Change the profile name for the account here
profileName = "housingprod"
session = boto3.Session(profile_name=profileName)
# Initialize session and DynamoDB
dynamodb = session.resource("dynamodb")
table: Table = dynamodb.Table("Assets")

# Scan the table for assetId and rentGroup
response = table.scan(ProjectionExpression="assetId, rentGroup")
items = response["Items"]

# Handle pagination
while "LastEvaluatedKey" in response:
    response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
    items.extend(response["Items"])

# Filter and clean data
filtered_data = [
    {"assetId": item["assetId"], "rentGroup": item.get("rentGroup", "")}
    for item in items
    if item.get("assetId") is not None
]

# Output based on selected format
if fileFormat == "csv":
    with open(f"assets_export_{profileName}.csv", mode="w", newline="") as file:
        writer = csv.DictWriter(
            file, fieldnames=["assetId", "rentGroup"], delimiter="\t"
        )
        writer.writeheader()
        writer.writerows(filtered_data)
    print(f"CSV file created: assets_export_{profileName}.csv")

elif fileFormat == "sql":
    with open(f"asset_rentgroup_ref_{profileName}.sql", mode="w") as file:
        for row in filtered_data:
            asset_id = str(row["assetId"]).replace("'", "''")
            rent_group = str(row["rentGroup"]).replace("'", "''")
            if rent_group == "":
                sql = f"INSERT INTO AssetRentGroupRefs (prop_ref, rentgroup) VALUES ('{asset_id}', NULL);\n"
            else:
                sql = f"INSERT INTO AssetRentGroupRefs (prop_ref, rentgroup) VALUES ('{asset_id}', '{rent_group}');\n"
            file.write(sql)
    print(f"SQL file created: asset_rentgroup_ref_{profileName}.sql")
