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
        PROCESSED_IDS_FILE = "output/processed_ids_set_isactive.csv"
    ASSETS_PER_SECOND = 8 / 1.5
    LIMIT = False  # Set to False when ready to run on all assets


def add_processed_id(asset_pk: str, success: bool, reason: str = ""):
    message = f"{asset_pk}, {success}, {reason}"
    Config.LOGGER.log(message)
    with open(Config.PROCESSED_IDS_FILE, "a") as f:
        f.write("\n" + message)


def date_is_past(date: str) -> bool:
    """Checks if a date is in the past"""
    current_date = time.strftime("%Y-%m-%d")
    return date < current_date


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


def set_is_active(raw_asset: dict) -> dict | None:
    """Sets the isActive attribute of the asset depending on the asset tenure start/end date"""
    if not raw_asset.get("rootAsset"):
        add_processed_id(raw_asset["id"], False, "Has no root asset - required field")
        return None
    try:
        if raw_asset.get("tenure") is None:
            raw_asset["isActive"] = False
        elif raw_asset["tenure"].get("endOfTenureDate") is None:
            raw_asset["isActive"] = True
        elif date_is_past(raw_asset["tenure"]["endOfTenureDate"]):
            raw_asset["isActive"] = False
        else:
            raw_asset["isActive"] = True
        return raw_asset
    except Exception as e:
        Config.LOGGER.log(f"Failed to process asset {raw_asset['id']} with error {e}")
        add_processed_id(raw_asset["id"], False, str(e))
        return None


def update_assets(asset_table: Table, updated_assets: list[Asset]):
    """update the asset record that are already assigned to a patch to have areaId and patchId and remove patches"""
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
        if "isActive" not in asset_datum:
            add_processed_id(asset_datum["id"], False, "Has no isActive property set")
            continue
        if asset_datum.get("isActive") not in [None, ""]:
            add_processed_id(asset_datum["id"], False, "Has a valid isActive attribute")
            continue
        fixed_asset = set_is_active(asset_datum)
        if fixed_asset is None:
            continue
        asset = Asset.from_data(fixed_asset)
        assets_to_update.append(asset)

    if confirm(f"Are you sure you want to update {len(assets_to_update)} assets in {Config.STAGE.to_env_name()}?"):
        update_assets(table, assets_to_update)
        Config.LOGGER.log(f"Updated assets out of {len(assets_to_update)}")


if __name__ == "__main__":
    main()
