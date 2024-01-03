from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.utils.logger import Logger
from aws.utils.progress_bar import ProgressBar
from enums.enums import Stage
from decimal import Decimal


@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_STAGING
    FILE_PATH = "aws/src/database/data/input/AdditionalAssetDataStaging.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }


def verify_integer(number: str | int | None) -> int:
    """
    Ensure input is valid integer
    """
    if isinstance(number, int):
        return number
    elif isinstance(number, str) and number.isdigit():
        return int(number)
    else:
        return None


def update_assets_with_additional_data(asset_table: Table, assets_from_csv: list[dict]) -> int:
    """
    update the asset record to have asset charateristics data given from csv

    """
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_csv))
    for i, csv_asset_item in enumerate(assets_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)
        asset_pk = csv_asset_item["property_pk"].strip()
        prop_ref = csv_asset_item["prop_ref"]
        no_of_bedrooms = csv_asset_item["num_bedrooms"]
        year_built = str(csv_asset_item["u_year_constructed"])
        rent_group = str(csv_asset_item["rentgrp_ref"])
        no_single_beds = csv_asset_item["no_single_beds"]
        no_double_beds = csv_asset_item["no_double_beds"]
        u_floors = csv_asset_item["u_floors"]
        u_marchetype = str(csv_asset_item["u_amarchetype"])
        u_block_floors = csv_asset_item["u_block_floors"]
        u_living_rooms = csv_asset_item["u_living_rooms"]
        u_window_type = str(csv_asset_item["u_window_type"])
        sc_factor = csv_asset_item["sc_factor"]

        try:
            sc_factor_decimal = Decimal(sc_factor)
        except:
            sc_factor_decimal = None

        heating = str(csv_asset_item["heating"])

        dynamo_asset = asset_table.get_item(Key={"id": asset_pk})
        dynamo_asset = dynamo_asset.get("Item")

        if not dynamo_asset.get("assetCharacteristics"):
            print(f"asset charateristic not available for {asset_pk}")
            dynamo_asset["assetCharacteristics"] = {}

        if not dynamo_asset.get("assetLocation"):
            print(f"asset location not available for {asset_pk}")
            dynamo_asset["assetLocation"] = {}

        if not dynamo_asset.get("assetLocation").get("parentAssets"):
            print(f"asset location parentAssets not available for {asset_pk}")
            dynamo_asset["assetLocation"] = {
                "parentAssets": []
            }


        dynamo_asset["assetCharacteristics"]["numberOfBedrooms"] = verify_integer(no_of_bedrooms)
        dynamo_asset["assetCharacteristics"]["yearConstructed"] = year_built if year_built else None
        dynamo_asset["assetCharacteristics"]["numberOfSingleBeds"] = verify_integer(no_single_beds)
        dynamo_asset["assetCharacteristics"]["numberOfDoubleBeds"] = verify_integer(no_double_beds)
        dynamo_asset["assetCharacteristics"]["windowType"] = u_window_type if u_window_type else None
        dynamo_asset["assetCharacteristics"]["numberOfFloors"] = verify_integer(u_floors)
        dynamo_asset["assetCharacteristics"]["architecturalType"] = u_marchetype if u_marchetype else None
        dynamo_asset["assetCharacteristics"]["numberOfLivingRooms"] = verify_integer(u_living_rooms)
        dynamo_asset["assetCharacteristics"]["propertyFactor"] = sc_factor_decimal if sc_factor_decimal else None
        dynamo_asset["assetCharacteristics"]["heating"] = heating if heating else None
        dynamo_asset["assetLocation"]["totalBlockFloors"] = verify_integer(u_block_floors)
        dynamo_asset["rentGroup"] = rent_group if rent_group else None

        dynamo_asset["assetCharacteristics"].pop("numberOfSingleBedrooms", None)
        dynamo_asset["assetCharacteristics"].pop("numberOfDoubleBedrooms", None)
        dynamo_asset["assetCharacteristics"].pop("floors", None)
        dynamo_asset["assetCharacteristics"].pop("rentGroup", None)

        asset_table.put_item(Item=dynamo_asset)
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
