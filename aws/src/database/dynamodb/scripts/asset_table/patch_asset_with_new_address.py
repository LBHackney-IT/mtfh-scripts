from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage
from decimal import Decimal


@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws\src\database\data\input\\new_address_data.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }

def update_assets_with_additional_data(asset_table: Table, assets_from_csv: list[dict]) -> int:
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_csv))
    for i, csv_asset_item in enumerate(assets_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)

        prop_ref = str(csv_asset_item["prop_ref"])
        new_addressLine1 = str(csv_asset_item["addressLine1"])
        new_addressLine2 = str(csv_asset_item["addressLine2"])
        new_addressLine3 = str(csv_asset_item["addressLine3"])
        new_addressLine4 = str(csv_asset_item["addressLine4"])
        new_postCode = str(csv_asset_item["postCode"])
        postPreamble = str(csv_asset_item["postPreamble"])
        
        # -- Dynamo fields
        # addressLine1 --> Field 'Dwelling Address VALIDATED'
        # addressLine2 --> The exact phrase "Hackney", unless field 'Ward' is "Out of Borough", in which case blank
        # addressLine3 --> The exact phrase "London"
        # addressLine4 --> delete all data in the field
        # postCode --> Field 'Postcode'
        # postPreamble --> delete all data in the field
        # uprn --> unchanged
        
        
        
        # TODO
        # Technical note: warn or error if the postcode is different to the existing record postcode


        # 1. Get asset from dynamoDb
        asset = get_by_secondary_index(asset_table, "AssetId", "assetId", prop_ref)[0]

        # 3. Add new address info to Address object
        asset["assetAddress"]["addressLine1"] = ''
        asset["assetAddress"]["addressLine2"] = ''
        asset["assetAddress"]["addressLine3"] = ''
        asset["assetAddress"]["addressLine4"] = ''
        asset["assetAddress"]["postCode"] = ''
        asset["assetAddress"]["postPreamble"] = ''

        # 4. Save changes back to asset
        asset_table.put_item(Item=asset)
        
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
