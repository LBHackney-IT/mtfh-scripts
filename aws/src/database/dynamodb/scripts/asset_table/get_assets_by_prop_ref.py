from dataclasses import dataclass
from typing import get_type_hints
import re

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.domain.dynamo_domain_objects import Asset
from aws.src.database.dynamodb.utils.dynamodb_item_factory import DynamodbItemFactory
from aws.src.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage


@dataclass
class Config:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.HOUSING_PRODUCTION
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }
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

def set_id_in_csv_with_asset_id(asset_table: Table, assets_from_csv: list[dict]) -> list[dict]:
    """
    Iterate through the asset and set the id for each record
    :return: ****TO UPDATE*** A dictionary containing the updated assets and the assets that failed to update
    """
    progress_bar = ProgressBar(len(assets_from_csv), bar_length=len(assets_from_csv) // 10)
    for i, asset_item in enumerate(assets_from_csv):
        if i % 10 == 0:
            progress_bar.display(i)
        asset_id = asset_item["Property Reference Number"]
        asset_id = clean_asset_id(asset_id)
        if asset_id is None:
            asset_item["failed_reason"] = f"Invalid assetId: {asset_item['Property Reference Number']}. " \
                                          f"Asset Address: {asset_item['Estate Name']}"
            continue
        results = get_by_secondary_index(asset_table, "AssetId", "assetId", asset_id)
        if len(results) > 1:
            asset_item["failed_reason"] = f"Multiple assets found for assetId {asset_item['Property Reference Number']}. " \
                                          f"Asset Address: {asset_item['Estate Name']}"
            continue
        elif len(results) == 0:
            asset_item["failed_reason"] = f"No assets found for assetId {asset_item['Property Reference Number']}. " \
                                          f"Asset Address: {asset_item['Estate Name']}"
            continue
        asset = results[0]
        assets_from_csv[i]["Id"] = asset.get("id")
        
    return assets_from_csv

def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = "aws\src\database\data\input\ProdAdditionalAssetData.csv"
    asset_csv_data = csv_to_dict_list(_file_path)

    # Note: Writing to TSV which can be imported into Google Sheets
    asset_with_ids = set_id_in_csv_with_asset_id(table, asset_csv_data)

    with open("assets.tsv", "w") as f:
        headings = asset_with_ids[0].keys()
        f.write("\t".join(headings) + "\n")
        for asset in asset_with_ids:
            f.write("\t".join([str(asset[heading]) for heading in headings]) + "\n")
