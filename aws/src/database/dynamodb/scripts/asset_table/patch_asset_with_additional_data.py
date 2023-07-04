from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage


@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_PRODUCTION
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
        asset_pk = csv_asset_item["property_pk"].strip()
        prop_ref = csv_asset_item["prop_ref"]
        no_of_bedrooms = csv_asset_item["num_bedrooms"]
        year_built = csv_asset_item["u_year_constructed"]
        rent_group = csv_asset_item["rentgrp_ref"]
        no_single_beds = csv_asset_item["no_single_beds"]
        no_double_beds = csv_asset_item["no_double_beds"]
        u_floors = csv_asset_item["u_floors"]
        u_marchetype = csv_asset_item["u_amarchetype"]
        u_block_floors = csv_asset_item["u_block_floors"]
        u_living_rooms = csv_asset_item["u_living_rooms"]
        u_window_type = csv_asset_item["u_window_type"]


        results = get_by_secondary_index(asset_table, "AssetId", "assetId", prop_ref)

        dynamo_asset = asset_table.get_item(Key={"id": asset_pk})
        dynamo_asset = dynamo_asset.get("Item")
        
        if not dynamo_asset.get("assetCharacteristics"):
            print(f"asset charateristic not available for {asset_pk}")
            dynamo_asset["assetCharacteristics"] = {}
            
        dynamo_asset["assetCharacteristics"]["numberOfBedrooms"] = no_of_bedrooms 
        dynamo_asset["assetCharacteristics"]["yearConstructed"] = year_built
        dynamo_asset["assetCharacteristics"]["numberOfSingleBedrooms"] = no_single_beds
        dynamo_asset["assetCharacteristics"]["numberOfDoubleBedrooms"] = no_double_beds 
        dynamo_asset["assetCharacteristics"]["blockFloors"] = u_block_floors 
        dynamo_asset["assetCharacteristics"]["windowType"] = u_window_type 
        dynamo_asset["assetCharacteristics"]["floors"] = u_floors 
        dynamo_asset["assetCharacteristics"]["architecturalType"] = u_marchetype
        dynamo_asset["assetCharacteristics"]["numberOfLivingRooms"] = u_living_rooms 
        dynamo_asset["assetCharacteristics"]["rentGroup"] = rent_group

        asset_table.put_item(Item=dynamo_asset)
        update_count += 1
    return update_count


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = "aws/src/database/data/input/Last704AdditionalAssetDataProd.csv"
    asset_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER

    # Note: Batch write to update the asset data in dynamodb
    update_count = update_assets_with_additional_data(table, asset_csv_data)
    logger.log(f"Updated {update_count} records")
