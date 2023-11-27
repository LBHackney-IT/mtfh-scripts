import os
from dataclasses import asdict, dataclass
from decimal import Decimal

from mypy_boto3_dynamodb.service_resource import Table
from boto3.dynamodb.types import TypeDeserializer
import json

from aws.src.database.domain.dynamo_domain_objects import Asset
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.progress_bar import ProgressBar
from aws.src.utils.logger import Logger
from enums.enums import Stage
from pathlib import Path

from utils.confirm import confirm


@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    if Path(__file__).parent.name == "mtfh-scripts":
        INPUT_FILE = "aws/src/database/dynamodb/scripts/asset_table/input/assetsDev.json"
        PROCESSED_IDS_FILE = "aws/src/database/dynamodb/scripts/asset_table/output/processed_ids.csv"
    else:
        INPUT_FILE = "input/assetsDev.json"
        PROCESSED_IDS_FILE = "aws/src/database/dynamodb/scripts/asset_table/output/processed_ids.csv"
    LIMIT = 1  # Set to False when ready to run on all assets


def add_processed_id(asset_pk: str, success: bool):
    print(f"Processed asset {asset_pk}")
    with open(Config.PROCESSED_IDS_FILE, "a") as f:
        f.write(f"{asset_pk}, {success}\n")


def load_assets(file_path: str) -> list[dict]:
    """Gets assets from a dynamodb json dump file and deserializes them to normal dictionaries"""
    _ds = TypeDeserializer()
    all_assets = []
    with open(Config.PROCESSED_IDS_FILE, "r") as f:
        processed_ids = [line.split(",")[0].strip() for line in f.readlines() if line.split(",")[1].strip() == "False"]
        print(f"Found {len(processed_ids)} processed ids out of {len(all_assets)} assets")

    with open(file_path, "r") as f:
        data = f.readlines()
    for asset_string in data:
        asset_raw = json.loads(asset_string)
        deserialised_asset = {k: _ds.deserialize(v) for k, v in asset_raw["Item"].items()}
        if deserialised_asset["id"] not in processed_ids:
            all_assets.append(deserialised_asset)
    if Config.LIMIT:
        all_assets = all_assets[:Config.LIMIT]
    return all_assets


def change_asset_schema(raw_asset: dict) -> dict | None:
    """
    Changes the schema of the asset to match the dynamodb schema
        - Removes patches attribute
        - Sets patchId and areaId attributes
    """
    if raw_asset.get("patches") is None:
        raw_asset["patchId"] = None
        raw_asset["areaId"] = None
    else:
        if len(raw_asset["patches"]) > 1 and Config.STAGE == Stage.HOUSING_PRODUCTION:
            Config.LOGGER.log(f"Prod Asset has more than one patch assigned to it, {raw_asset['id']}, skipping")
            return None
        patch = [poa_raw for poa_raw in raw_asset["patches"] if poa_raw["patchType"] == "patch"][0]
        raw_asset["patchId"] = patch["id"] if patch else None
        raw_asset["areaId"] = patch["parentId"] if patch else None
        raw_asset.pop("patches", None)
    return raw_asset


def update_assets(asset_table: Table, updated_assets: list[Asset]) -> int:
    """
    update the asset record that are already assigned to a patch to have areaId and patchId and remove patches
    """
    update_count = 0
    progress_bar = ProgressBar(len(updated_assets))
    for i, asset_item in enumerate(updated_assets):
        if i % 100 == 0:
            progress_bar.display(i)
        try:
            asset_item.versionNumber = asset_item.versionNumber + 1 if asset_item.versionNumber else 0
            asset_table.put_item(Item=asdict(asset_item))
            update_count += 1
            add_processed_id(asset_item.id, True)
        except Exception as e:
            Config.LOGGER.log(f"Failed to update asset {asset_item.id} with error {e}")
            add_processed_id(asset_item.id, False)
    return update_count


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)

    assets_to_update = []
    asset_data = load_assets(Config.INPUT_FILE)
    for asset_datum in asset_data:
        if asset_datum.get("patches") is None and asset_datum.get("patchId") is not None:
            print(f"Asset {asset_datum['id']} has patchId but no patches, skipping")
            continue
        fixed_asset = change_asset_schema(asset_datum)
        asset = Asset.from_data(fixed_asset)
        assets_to_update.append(asset)

    if confirm(f"Are you sure you want to update {len(assets_to_update)} assets in {Config.STAGE.to_env_name()}?"):
        update_count = update_assets(table, assets_to_update)
        Config.LOGGER.log(f"Updated {update_count} assets out of {len(assets_to_update)}")


if __name__ == "__main__":
    main()
