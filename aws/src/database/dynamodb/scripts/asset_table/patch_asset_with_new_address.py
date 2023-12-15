from dataclasses import dataclass
import sys

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage
from decimal import Decimal
import requests


@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws\src\database\input\\RELOAD.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }
    ASSET_API_URL = "<REDACTED>"
    ASSET_API_KEY = "<REDACTED>"

def update_assets_with_additional_data(asset_table: Table, assets_from_csv: list[dict]) -> int:
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_csv))
    for i, csv_asset_item in enumerate(assets_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)

        lookup_prop_ref = str(csv_asset_item["assetId"])
        lookup_uprn = str(csv_asset_item["uprn"])
        new_addressLine1 = str(csv_asset_item["addressLine1"])

        # 1. Agreed rule - if the Ward is out of borough, state this on the address, 
        new_addressLine2 = 'Hackney'
        if str(csv_asset_item["addressLine2"]) == 'Out of Borough':
            new_addressLine2 = 'Out of Borough'

        new_addressLine3 = 'London'

        new_addressLine4 = str(csv_asset_item["addressLine4"])
        new_postCode = str(csv_asset_item["postCode"])
        
        # -- Dynamo fields agreed mapping
        # addressLine1 --> Field 'Dwelling Address VALIDATED'
        # addressLine2 --> The exact phrase "Hackney", unless field 'Ward' is "Out of Borough", in which case blank
        # addressLine3 --> The exact phrase "London"
        # addressLine4 --> delete all data in the field
        # postCode --> Field 'Postcode'
        # postPreamble --> delete all data in the field
        # uprn --> unchanged

        # 2. Get asset from dynamoDb
        asset = get_by_secondary_index(asset_table, "AssetId", "assetId", lookup_prop_ref)[0]

        # 3a. Check for postcode congruance - if it's not the same, it's likely to be an incorrect dataset
        if asset["assetAddress"]["postCode"] != new_postCode:
            Logger.log(f"Warning: new record contains {new_postCode} which is different from existing postcode {asset['assetAddress']['postCode']}")
            sys.exit()

        # 3b. Check for UPRN congruance - if it's not the same, it's likely to be an incorrect dataset
        if lookup_uprn != asset["assetAddress"]["uprn"]:
            Logger.log(f"Warning: new record contains {lookup_uprn} which is different from existing uprn {asset['assetAddress']['uprn']}")
            sys.exit()

        # 4 Create a request to assets API
        #url = str(Config.ASSET_API_URL) + str(r"assets") + str('/' + asset["id"]) + str("/address")
        url = f'{Config.ASSET_API_URL}assets/{asset["id"]}/address'
        editPayload = {
            "assetAddress": {
                "uprn": lookup_uprn,
                "addressLine1": new_addressLine1,
                "addressLine2": new_addressLine2,
                "addressLine3": new_addressLine3,
                "addressLine4": new_addressLine4,
                "postCode": new_postCode,
                "postPreamble": ""
            }
        }

        bearer_token = Config.ASSET_API_KEY

        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json"
        }

        # 5 alt. send to asset api
        response = requests.patch(url, json=editPayload, headers=headers)
        
        if response.status_code == 200:
            Logger.log(f"Record update successful!")
            update_count += 1
    return update_count


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = Config.FILE_PATH
    asset_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER

    # Note: Batch write to update the asset data in dynamodb
    update_count = update_assets_with_additional_data(table, asset_csv_data)
    logger.log(f"Updated {update_count} records")
