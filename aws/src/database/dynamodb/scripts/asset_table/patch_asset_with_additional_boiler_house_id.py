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
    STAGE = Stage.HOUSING_STAGING
    FILE_PATH = "aws\src\database\data\input\\boiler_house_data.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }

def update_assets_with_additional_data(asset_table: Table, assets_from_csv: list[dict]) -> int:
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_csv), bar_length=len(assets_from_csv) // 10)
    for i, csv_asset_item in enumerate(assets_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)

        prop_ref = str(csv_asset_item["prop_ref"])
        boiler_house_prop_ref = str(csv_asset_item["boiler"])

        # 1. Get asset from dynamoDb
        asset = get_by_secondary_index(asset_table, "AssetId", "assetId", prop_ref)[0]

        # 2. Get assetGuid for boilerHouse
        boilerHouseGuidPk = get_by_secondary_index(asset_table, "AssetId", "assetId", boiler_house_prop_ref)[0]['id']

        # 3. Add boilerHouseId to asset object
        asset["boilerHouseId"] = boilerHouseGuidPk

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
