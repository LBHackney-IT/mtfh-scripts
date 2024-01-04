from dataclasses import dataclass

from aws.utils.logger import Logger
from aws.utils.progress_bar import ProgressBar
from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from mypy_boto3_dynamodb.service_resource import Table

import boto3
import json
import datetime
import uuid

@dataclass
class Config:
    LOGGER = Logger()
    STAGE = Stage.HOUSING_PRODUCTION
    TABLE_NAME = "Assets"
    FILE_PATH = "aws\src\elastic_search\data\input\property_data.csv"
    SNS_TOPIC_ARN = input("Enter SNS topic ARN: ")

@dataclass
class User:
    NAME = "Callum Macpherson"
    EMAIL = "callum.macpherson@hackney.gov.uk"

def setup_client() -> boto3.client:
    return generate_aws_service("sns", Config.STAGE)

def generate_message(assetId):
    sns_message = {
        "id": str(uuid.uuid4()),
        "eventType": "AssetUpdatedEvent",
        "sourceDomain": "Asset",
        "sourceSystem": "AssetAPI",
        "version": "v1",
        "correlationId": str(uuid.uuid4()),
        "dateTime": str(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")),
        "user": {
            "name": User.NAME,
            "email": User.EMAIL,
        },
        "entityId": assetId,
        "eventData": {},
    }

    return json.dumps(sns_message)

def update_assets(asset_table: Table, properties_from_csv: list[dict]) -> int:
    logger = Config.LOGGER

    update_count = 0
    progress_bar = ProgressBar(len(properties_from_csv), bar_length=len(properties_from_csv) // 10)

    sns_client = setup_client()

    for i, csv_property_item in enumerate(properties_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)

        prop_ref = str(csv_property_item["property_number"])

        try:
            update_asset(prop_ref, asset_table, sns_client)

        except Exception as e:
            logger.log(f"Failed to update asset with propRef {prop_ref} with exception {str(e)}")

        else:
            # Success
            update_count += 1

    return update_count

def update_asset(prop_ref, asset_table, sns_client):
    logger = Config.LOGGER

    # 1. Get asset from dynamoDb
    assets = get_by_secondary_index(asset_table, "AssetId", "assetId", prop_ref)

    # no assets found with matching property reference
    if not assets:
        logger.log(f"Asset with propRef {prop_ref} not found in asset table")
        return

    asset = assets[0]

    # 2. Generate Message
    sns_message = generate_message(asset['id'])

    # 3. Publish SNS message
    response = sns_client.publish(
        TopicArn=Config.SNS_TOPIC_ARN,
        Message=sns_message,
        MessageGroupId="fake",
    )

    # Log ids for failed requests
    if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
        logger.log(f"Request failed for asset {asset.id} with response {response['ResponseMetadata']['HTTPStatusCode']}")

def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    property_csv_data = csv_to_dict_list(Config.FILE_PATH)

    update_count = update_assets(table, property_csv_data)

    logger = Config.LOGGER

    logger.log(f"Updated {update_count} records")
