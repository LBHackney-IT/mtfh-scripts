from dataclasses import asdict, dataclass

from mypy_boto3_dynamodb.service_resource import Table
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
import json

from aws.src.database.domain.dynamo_domain_objects import Asset
from aws.src.database.dynamodb.utils.dynamodb_item_factory import DynamodbItemFactory
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.progress_bar import ProgressBar
from aws.src.utils.logger import Logger
from enums.enums import Stage

@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws/src/database/dynamodb/scripts/asset_table/input/assetsDev.json"


def load_assets(file_path: str) -> list[Asset]:
    """
    """
    #file context manager
    _ds = TypeDeserializer()
    all_assets: list[Asset] = []
    with open(file_path, "r") as f:
        # one object on each line
        data = f.readlines()
        for asset_string in data:
            asset_raw = json.loads(asset_string)
            deserialised_asset = {k: _ds.deserialize(asset_raw[k]) for k in asset_raw}
            asset = Asset.from_data(deserialised_asset)
            all_assets.append(asset) 
            

def update_assets(asset_table: Table, assets_from_file: list[Asset]) -> int:
    """
    update the asset record that are already assigned to a patch to have areaId and patchId and remove patches
    """
    logger = Config.LOGGER
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_file))
    for i, asset_item in enumerate(assets_from_file):
        if i % 100 == 0:
            progress_bar.display(i)
        asset_pk = asset_item.id.strip()

        if not asset_item.patches:
            print(f"property doesn't have a patch assigned to it {asset_pk}")
        else:
            print(f"I am here {asset_pk}")
            if len(asset_item.patches) > 1:
                if Config.STAGE == Stage.HOUSING_PRODUCTION:
                    continue
                logger.log(f"Asset has more than one patch assigned to it {asset_pk}")
                asset_item.patches = asset_item.patches[0:1]
    
            if asset_item.versionNumber is None:
                asset_item.versionNumber = 0
            else:
                asset_item.versionNumber += 1

            for patch in asset_item.patches:
                patchId = patch.id
                areaId = patch.parentId
                asset_item["areaId"] = areaId if areaId else None
                asset_item["patchId"] = patchId if patchId else None
                asset_item["patches"].pop("patches", None)
        asset_table.put_item(Item=asdict(asset_item))
        update_count += 1
    return update_count


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)    
    logger = Config.LOGGER
    _file_path = Config.FILE_PATH
    asset_csv_data = csv_to_dict_list(_file_path)

    # Note: Batch write to update the asset data in dynamodb
    update_count = update_assets(table, asset_csv_data)
    logger.log(f"Updated {update_count} records")
