from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.utils.logger import Logger
from aws.utils.progress_bar import ProgressBar
from enums.enums import Stage

@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws/src/database/data/input/LBH-Owned-Assets.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }

# IMPORTANT: See logs\logs.txt for information about the output of the script

def change_asset_ownership_to_lbh(asset_table: Table, assets_from_csv: list[dict], logger: Logger) -> int:
    progress_bar = ProgressBar(len(assets_from_csv))

    # The array below will be used to display a list of assets that could not be found
    assets_not_found = []
    assets_changed = []

    for i, csv_asset_item in enumerate(assets_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)

        # Get AssetId/PropRef from CSV file
        asset_prop_ref = str(csv_asset_item["PropRef"])

        # Get asset object, using the AssetId, from DynamoDb
        db_data_retrieve = get_by_secondary_index(asset_table, "AssetId", "assetId", asset_prop_ref)

        if (len(db_data_retrieve) > 0):
            asset_record = db_data_retrieve[0]

            # Property assetManagement is NOT present (meaning property isCouncilProperty is also NOT present)
            if ("assetManagement" not in asset_record or asset_record['assetManagement'] is None):

                # Add new assetManagement property to asset, and within assetManagement, set isCouncilProperty to True
                asset_record['assetManagement'] = {"isCouncilProperty" : True}
                change_asset_ownership(asset_table, asset_record, assets_changed, logger)

            else:
                # Property assetManagement is present and property isCouncilProperty is NOT present
                if ('isCouncilProperty' not in asset_record['assetManagement']):
                    # We create isCouncilProperty within assetManagement (without affecting other properties within assetManagement)
                    asset_record["assetManagement"]["isCouncilProperty"] = True
                    change_asset_ownership(asset_table, asset_record, assets_changed, logger)

                else:
                    #  Property isCouncilProperty is present and set to True
                    if (asset_record["assetManagement"]["isCouncilProperty"] == True):
                        # Nothing to do. Move onto next item in CSV file.
                        continue

                    #  Property isCouncilProperty is present and set to False
                    elif (asset_record["assetManagement"]["isCouncilProperty"] == False):
                        # Create new/change property isCouncilProperty to True (without affecting other properties within assetManagement)
                        asset_record["assetManagement"]["isCouncilProperty"] = True
                        change_asset_ownership(asset_table, asset_record, assets_changed, logger)

        else:
            assets_not_found.append(asset_prop_ref)
            logger.log(f'Could not find asset with prop ref {asset_prop_ref}')

    logger.log("Script execution complete")
    return assets_changed, assets_not_found


def change_asset_ownership(asset_table, asset_record, assets_changed, logger):
    assets_changed.append(asset_record["assetId"])
    asset_table.put_item(Item=asset_record)
    logger.log(f"Ownership of asset with prop ref {asset_record['assetId']} has been changed to LBH.")

def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = Config.FILE_PATH
    asset_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER

    assets_changed, assets_not_found = change_asset_ownership_to_lbh(table, asset_csv_data, logger)

    # Display final output
    if (len(assets_changed) > 0):
        logger.log(f"A total of {len(assets_changed)} assets have been changed, and are now recorded as LBH properties in the database.")
        logger.log("Assets changed:")
        logger.log(assets_changed)
    else:
        logger.log(f"No assets were changed.")

    if (len(assets_not_found) > 0):
        logger.log("The following assets could not be found in the database:")
        logger.log(assets_not_found)
