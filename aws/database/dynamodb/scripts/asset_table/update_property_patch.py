from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.utils.logger import Logger
from enums.enums import Stage
from aws.utils.progress_bar import ProgressBar
from aws.database.domain.dynamo_domain_objects import Asset

import requests
import os
import re
from dotenv import load_dotenv
from utils.confirm import confirm


load_dotenv()


@dataclass
class Config:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.HOUSING_STAGING
    ITEM_COUNT_LIMIT = 10  # Set to None to return all items


def clean_asset_id(asset_id: str) -> str | None:
    asset_id = str(asset_id)
    asset_id = asset_id.replace(" ", "")
    if len(asset_id) < 8:
        # pad with zeros
        asset_id = asset_id.zfill(8)
    if asset_id.isnumeric():
        if len(asset_id) > 8:
            return None
        propref_regex = re.compile(r"^([0-9]{8})$")
        asset_id = propref_regex.findall(asset_id)[0]
        return asset_id
    return None


def update_areaid_patchid_in_asset(assets_table: Table, patch_from_csv: list[dict], logger: Logger):
    # get all patches and areas endpoint
    url = os.getenv('GETALLPATCHESANDAREAAPIURL')
    token = os.getenv('AUTH_TOKEN')
    if None not in (url, token):
        response = requests.get(url, headers={'Authorization': token})
        allPatchesAndAreas = response.json()

    progress_bar = ProgressBar(len(patch_from_csv))
    # get asset record based on prop ref from csv
    for i, item in enumerate(patch_from_csv):
        if i % 10 == 0:
            progress_bar.display(i)
        asset_id = item["prop_ref"]
        asset_id = clean_asset_id(asset_id)
        if asset_id is None:
            logger.log(f"Invalid assetId: {item['prop_ref']}. ")
            continue
        results = get_by_secondary_index(
            assets_table, "AssetId", "assetId", asset_id)
        if len(results) > 1:
            logger.log(
                f"Multiple assets found for assetId {item['prop_ref']}. ")
            continue
        elif len(results) == 0:
            logger.log(f"No assets found for assetId {item['prop_ref']}. ")
            continue
        asset = results[0]
        patchName = item["name"]
        if not patchName:
            logger.log(f'patchName is not given for propertyRef {asset_id}')
            continue
        # //get patch object based on patch name given in csv
        for patch in allPatchesAndAreas:
            if patch['name'] == patchName:
                patchId = patch['id']
                areaId = patch['parentId']
                asset["patchId"] = patchId if patchId else None
                asset["areaId"] = areaId if areaId else None
                logger.log(
                    f'updating areaid to {asset["areaId"]} and patchId to {asset["patchId"]} for prop_ref {asset_id}')
                assets_table.put_item(Item=asset)
                logger.log(f"UPDATED {asset['id']}")


def update_property_patch():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = r"/workspaces/mtfh-scripts/aws/Propert Patch List for script STAGING.csv"
    patch_csv_data = csv_to_dict_list(_file_path)

    if confirm(f"Are you sure you want to update property patch in {Config.STAGE.to_env_name()}?"):
        update_areaid_patchid_in_asset(
            table, patch_csv_data, Config.LOGGER)


if __name__ == "__main__":
    update_property_patch()
