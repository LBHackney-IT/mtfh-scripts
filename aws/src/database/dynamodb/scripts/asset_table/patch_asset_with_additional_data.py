from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage


@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }


def verify_number(number: str | int) -> bool:
    """
    Verify if the input is a valid integer
    """
    if isinstance(number, int):
        return True
    elif isinstance(number, str) and number.isdigit():
        return True
    else:
        return False


def update_assets_with_additional_data(asset_table: Table, assets_from_csv: list[dict]) -> int:
    """
    update the asset record to have asset charateristics data given from csv
    
    """
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_csv), bar_length=len(assets_from_csv) // 10)
    for i, csv_asset_item in enumerate(assets_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)
        asset_pk = csv_asset_item["Id"].strip()
        no_of_bedrooms = csv_asset_item["Dwelling No. of bedrooms"]
        year_built = csv_asset_item["Year of Built"]

        dynamo_asset = asset_table.get_item(Key={"id": asset_pk}).get("Item")

        dynamo_asset["assetCharacteristics"]["numberOfBedrooms"] = no_of_bedrooms if verify_number(no_of_bedrooms) else \
        dynamo_asset["assetCharacteristics"].get("numberOfBedrooms")
        dynamo_asset["assetCharacteristics"]["yearConstructed"] = year_built if verify_number(year_built) else \
        dynamo_asset["assetCharacteristics"].get("yearConstructed")

        asset_table.put_item(Item=dynamo_asset)
        update_count += 1
    return update_count


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = "aws\src\database\data\input\DevUpdateAssetData.csv"
    asset_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER

    # Note: Batch write to update the asset data in dynamodb
    update_count = update_assets_with_additional_data(table, asset_csv_data)
    logger.log(f"Updated {update_count} records")
