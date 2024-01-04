"""
To set this script up locally, you will need to export the Assets DynamoDB table to JSON, see guide:
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html

The Config.INPUT_FILE should be set to the path of the exported DynamoDB JSON file
"""

from dataclasses import asdict, dataclass
import time

from mypy_boto3_dynamodb.service_resource import Table
from boto3.dynamodb.types import TypeDeserializer
import json

from aws.database.domain.dynamo_domain_objects import Asset
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.progress_bar import ProgressBar
from aws.utils.logger import Logger
from enums.enums import Stage
from pathlib import Path

from utils.confirm import confirm


@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    if Path(__file__).parent.name == "mtfh-scripts":
        INPUT_FILE = "aws/src/database/dynamodb/scripts/asset_table/input/assetsStaging.json"
        PROCESSED_IDS_FILE = "aws/src/database/dynamodb/scripts/asset_table/output/processed_ids.csv"
    else:
        INPUT_FILE = "input/assetsDev.json"
        PROCESSED_IDS_FILE = "output/processed_ids.csv"
    ASSETS_PER_SECOND = 8 / 1.5
    LIMIT = False  # Set to False when ready to run on all assets


def add_processed_id(asset_pk: str, success: bool, reason: str = ""):
    message = f"{asset_pk}, {success}, {reason}"
    Config.LOGGER.log(message)
    with open(Config.PROCESSED_IDS_FILE, "a") as f:
        f.write("\n" + message)


def load_assets(file_path: str) -> list[dict]:
    """Gets assets from a dynamodb json dump file and deserializes them to normal dictionaries"""
    _ds = TypeDeserializer()
    all_assets = []

    with open(file_path, "r") as f:
        data = f.readlines()

    with open(Config.PROCESSED_IDS_FILE, "r") as f:
        lines = f.readlines()
        processed_ids = [line.split(",")[0].strip() for line in lines]
        print(f"Found {len(processed_ids)} processed ids out of {len(data)} assets")

    for asset_string in data:
        asset_raw = json.loads(asset_string)
        deserialised_asset = {k: _ds.deserialize(v) for k, v in asset_raw["Item"].items()}
        if deserialised_asset["id"] not in processed_ids:
            all_assets.append(deserialised_asset)
    if Config.LIMIT:
        all_assets = all_assets[:Config.LIMIT]
    print(f"Processing {len(all_assets)} out of {len(data)} assets")
    return all_assets


def change_asset_schema(raw_asset: dict) -> dict | None:
    """
    Changes the schema of the asset to match the dynamodb schema
        - Removes patches attribute
        - Sets patchId and areaId attributes
    """
    if not raw_asset.get("rootAsset"):
        add_processed_id(raw_asset["id"], False, "Has no root asset - required field")
        return None
    if raw_asset.get("patches") in [None, []]:
        add_processed_id(raw_asset["id"], False, f"No patches assigned: {raw_asset.get('patches')}")
        return None
    try:
        patches: list[dict] = [patch for patch in raw_asset["patches"] if patch.get("patchType") == "patch"]
        patch = patches[0] if patches else None
        if patch is None:
            Config.LOGGER.log(f"Asset has no patch assigned, {raw_asset['id']}, skipping")
            return None
        raw_asset["patchId"] = patch["id"] if patch.get("id") else None
        raw_asset["areaId"] = patch["parentId"] if patch.get("parentId") else None
        raw_asset.pop("patches", None)
        return raw_asset
    except Exception as e:
        Config.LOGGER.log(f"Failed to process asset {raw_asset['id']} with error {e}")
        add_processed_id(raw_asset["id"], False, str(e))
        return None


def update_assets(asset_table: Table, updated_assets: list[Asset]):
    """
    update the asset record that are already assigned to a patch to have areaId and patchId and remove patches
    """
    delay_per_asset = round(1 / Config.ASSETS_PER_SECOND, 2)
    print(f"Processing {Config.ASSETS_PER_SECOND} assets per second with a delay of {delay_per_asset} seconds per asset")
    progress_bar = ProgressBar(len(updated_assets))
    for i, asset_item in enumerate(updated_assets):
        if i % 100 == 0:
            progress_bar.display(i)
        try:
            asset_item.versionNumber = asset_item.versionNumber + 1 if asset_item.versionNumber else 0
            asset_table.put_item(Item=asdict(asset_item))
            add_processed_id(asset_item.id, True)
            time.sleep(delay_per_asset)
        except Exception as e:
            Config.LOGGER.log(f"Failed to update asset {asset_item.id} with error {e}")
            add_processed_id(asset_item.id, False, str(e))
    return


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)

    assets_to_update = []
    asset_data = load_assets(Config.INPUT_FILE)
    for asset_datum in asset_data:
        if asset_datum.get("patches") is None and asset_datum.get("patchId") is not None:
            print(f"Asset {asset_datum['id']} has patchId but no patches, skipping")
            continue
        fixed_asset = change_asset_schema(asset_datum)
        if fixed_asset is None:
            continue
        asset = Asset.from_data(fixed_asset)
        assets_to_update.append(asset)

    if confirm(f"Are you sure you want to update {len(assets_to_update)} assets in {Config.STAGE.to_env_name()}?"):
        update_assets(table, assets_to_update)
        Config.LOGGER.log(f"Updated assets out of {len(assets_to_update)}")


if __name__ == "__main__":
    main()
