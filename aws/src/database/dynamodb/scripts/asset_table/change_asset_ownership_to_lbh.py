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
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws/src/database/data/input/LBH-Owned-Assets.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }

# IMPORTANT: See logs\logs.txt for information about the output of the script

def change_asset_ownership_to_lbh(asset_table: Table, assets_from_csv: list[dict], logger: Logger) -> int:
    progress_bar = ProgressBar(len(assets_from_csv))

    # The array below will be used to display a list of assets that could not be found
    no_asset_found = []
    assets_changed = []
    no_ownership = []

    for i, csv_asset_item in enumerate(assets_from_csv):        
        if i % 100 == 0:
            progress_bar.display(i)

        # Get AssetId/PropRef from CSV file
        asset_prop_ref = str(csv_asset_item["PropRef"])

        # Get asset object, using the AssetId, from DynamoDb
        db_data_retrieve = get_by_secondary_index(asset_table, "AssetId", "assetId", asset_prop_ref)

        if (len(db_data_retrieve) > 0):
            asset_record = db_data_retrieve[0]

            # Check if asset has assetManagement property
            if ("assetManagement" not in asset_record):
                # if not, create it and assign ownership to LBH
                asset_record['assetManagement'] = {"isCouncilProperty" : True}

            # Otherwise
            else:
                # check if isCouncilProperty is false
                if (asset_record["assetManagement"]["isCouncilProperty"] == False):
                    # if yes, change it to true
                    asset_record["assetManagement"]["isCouncilProperty"] = True
                    
                    
                    assets_changed.append(asset_prop_ref)
                    
                    # save changed record to database
                    asset_table.put_item(Item=asset_record)
                    logger.log(f"Ownership of asset with prop ref {asset_prop_ref} has been changed to LBH.")
                    
                    # otherwise move onto next item in CSV file
                else: 
                    continue


        else:
            no_asset_found.append(asset_prop_ref)
            logger.log(f'Could not find asset with prop ref {asset_prop_ref}')
    
    # Display assets that could not be found
    if (len(no_asset_found) > 0):
        logger.log("Script completed. The following assets could not be found in the database:")
        logger.log(no_asset_found)

    return assets_changed


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = Config.FILE_PATH
    asset_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER

    # Note: Batch write to update the asset data in dynamodb
    assets_changed = change_asset_ownership_to_lbh(table, asset_csv_data, logger)

    if (len(assets_changed) > 0):
        logger.log(f"Script execution complete. A total of {len(assets_changed)} assets have been changed, and are now recorded as LBH properties in the database.")
        logger.log("Assets changed:")
        logger.log(assets_changed)
    else:
        logger.log(f"Script execution complete. No assets were changed.")
