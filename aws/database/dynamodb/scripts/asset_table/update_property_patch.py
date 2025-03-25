# 1. Get asset record by asset id (prop ref) - api endpoint 
# 2. Get patch record by patch name on csv - api endpoint 
# 3. update patch id and area id in asset record to match patch on csv 
# 4. loop for all records in csv
from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from get_assets_by_prop_ref import main
from aws.utils.logger import Logger
from enums.enums import Stage
from aws.utils.progress_bar import ProgressBar
from aws.database.domain.dynamo_domain_objects import Patch
from aws.database.domain.dynamo_domain_objects import Asset



@dataclass
class Config:
    TABLE_NAME = "PatchesAndAreas"
    OUTPUT_CLASS = Patch
    LOGGER = Logger()
    STAGE = Stage.HOUSING_STAGING
    ITEM_COUNT_LIMIT = 10  # Set to None to return all items

class AssetConfig:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.HOUSING_STAGING
    ITEM_COUNT_LIMIT = 10  # Set to None to return all items

def get_patch_record_by_patch_name(patchesAndAreas_table: Table, patch_from_csv: list[dict]) -> list[dict]:
    """
    Iterate through the asset and set the id for each record
    :return: ****TO UPDATE*** A dictionary containing the updated assets and the assets that failed to update
    """
    progress_bar = ProgressBar(len(patch_from_csv))

    for i, patch_item in enumerate(patch_from_csv):
        if isinstance(patch_item["patch_pk"], str) and len(patch_item["patch_pk"]) > 0:
            continue
        else:
            if i % 10 == 0:
                progress_bar.display(i)
            patch_name = patch_item["name"]
            if patch_name is None:
                patch_name["failed_reason"] = f"Invalid patchName: {patch_item['name']}. "
                continue
            results = get_by_secondary_index(patchesAndAreas_table, "patchName", "name", patch_name)
            if len(results) > 1:
                patch_item["failed_reason"] = f"Multiple patches found for patchName {patch_item['name']}. "
                continue
            elif len(results) == 0:
                patch_item["failed_reason"] = f"No patch found for patchName {patch_item['name']}. "
                continue
            # update patch id and area id in asset record to match patch on csv
            patch = results[0]
            patch_id = patch.get("id")
            area_id = patch.get("parentId")
            prop_ref = patch_item["prop_ref"]
            table = get_dynamodb_table(AssetConfig.TABLE_NAME, AssetConfig.STAGE)
            update_areaid_patchid_in_asset(patch_id, area_id, prop_ref, table, AssetConfig.LOGGER)
            patch_from_csv[i]["patch_pk"] = patch.get("id")

    return patch_from_csv

def update_areaid_patchid_in_asset(patch_id: str | None, area_id: str | None, prop_ref :str, assets_table: Table, logger: Logger):
    if len(prop_ref) is None:
        logger.log(f'Could not find asset with prop ref {prop_ref}') 
    asset = get_by_secondary_index(assets_table, "AssetId", "assetId", prop_ref)
    if (len(asset) > 0):
            asset_record = asset[0]
            asset_record["patchId"] = patch_id if patch_id else None
            asset_record["areaId"] = area_id if area_id else None
            logger.log(f'updating areaid to {asset_record["areaId"]} and patchId to {asset_record["patchId"]} for prop_ref {prop_ref}')
            assets_table.put_item(Item=asset_record)



def update_property_patch():
    # 1. Get asset record by asset id (prop ref) 
    assets_by_prop_ref = main()
    # 2. Get patch record by patch name on csv - api endpoint 
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = r"aws\src\database\data\input\update_property_patch_march_2025.csv"
    patch_csv_data = csv_to_dict_list(_file_path)
    get_by_patch_name = get_patch_record_by_patch_name(table, patch_csv_data)

if __name__ == "__main__":
    update_property_patch()