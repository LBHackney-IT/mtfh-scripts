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
    FILE_PATH = "aws/src/database/data/input/property_table_full.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }

# IMPORTANT: It is advised to open a specific terminal window prior to running this script,
# so that after the script has run, the terminal will not close, and you will be able to view 
# the final output (displaying any assets that, for whatever reason, -could not be updated).

def update_assets_with_parents_data(asset_table: Table, assets_from_csv: list[dict], logger: Logger) -> int:
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_csv))

    # The arrays below will be used to display lists of assets that could not be
    # modified by the script (asset not found in DynamoDb).
    no_asset_found_children = []
    no_asset_found_parents = []

    for i, csv_asset_item in enumerate(assets_from_csv):        
        if i % 100 == 0:
            progress_bar.display(i)

        # Get AssetId/PropRef from CSV file
        child_asset_prop_ref = str(csv_asset_item["property_number"])

        # Check if AssetId/PropRef exists for parent asset, if not move onto the next line in the CSV file
        if str(csv_asset_item["parent"]) == "":
            continue

        # If AssetId is less than 8 digits, left pad it with 0s until AssetId is composed of 8 digits
        # if len(child_asset_prop_ref) < 8:
        #     child_asset_prop_ref = child_asset_prop_ref.rjust(8, '0')

        # Get asset object, using the AssetId, from DynamoDb
        data_retrieve_child = get_by_secondary_index(asset_table, "AssetId", "assetId", child_asset_prop_ref)

        if (len(data_retrieve_child) > 0):
             # If we successfully retrieve data, we can access the (child) access object
            child_asset_record = data_retrieve_child[0]

            # Get the parent from the CSV file for the asset
            parent_asset_prop_ref = str(csv_asset_item["parent"])

                # If AssetId is less than 8 digits, left pad it with 0s until AssetId is composed of 8 digits
                    # if len(parent_asset_prop_ref) < 8:
                    #     parent_asset_prop_ref = parent_asset_prop_ref.rjust(8, '0')

            # If the AssetId is "00087086", we know that's "Hackney Homes" and we can use the below details (taken from Housing-Production - Assets table)
            if parent_asset_prop_ref == "00087086":
                parent_asset_guid = "656feda1-896f-b136-da84-163ee4f1be6c"
                parent_asset_name = "Hackney Homes"
                parent_asset_type = "NA"
            else:
                # Alternatively, we fetch asset object, using the AssetId, from DynamoDb, for the parent asset
                data_retrieve_parent = get_by_secondary_index(asset_table, "AssetId", "assetId", parent_asset_prop_ref)

                # If we have results, get the first object and get its GUID, Address Line 1 (name) and its asset type.
                if (len(data_retrieve_parent) > 0):
                    parent_asset_guid = data_retrieve_parent[0]['id']
                    parent_asset_name = data_retrieve_parent[0]['assetAddress']['addressLine1']
                    parent_asset_type = data_retrieve_parent[0]['assetType']

                # If not output error message and make note of the asset for which a parent CANNOT be found, and move onto the next line in the CSV file
                else:
                    no_asset_found_parents.append(child_asset_prop_ref)
                    logger.log(f'Cannot find (parent) asset for Asset ID {child_asset_prop_ref}')
                    continue

            # Now we set "parentAssetIds" field of the child asset record to the value of parent_asset_guid
            child_asset_record["parentAssetIds"] = parent_asset_guid

            # And we set the "parentAssets" property as well
            # We make sure the "assetLocation" is present in the asset first
            if "assetLocation" in child_asset_record:
                child_asset_record["assetLocation"]["parentAssets"] = [
                    {
                        "id": parent_asset_guid,
                        "name": parent_asset_name,
                        "type": parent_asset_type
                    },
                ]
            else: 
            # If not, we create the property first (Python dict eq to DynamoDb Map)
                child_asset_record["assetLocation"] = {}
                child_asset_record["assetLocation"]["parentAssets"] = [
                    {
                        "id": parent_asset_guid,
                        "name": parent_asset_name,
                        "type": parent_asset_type
                    },
                ]

            # Once we have amended the (child) asset with parent information on both fields, we save the changes
            asset_table.put_item(Item=child_asset_record)

            update_count += 1
        else:
            no_asset_found_children.append(child_asset_prop_ref)
            logger.log(f'Cannot find (child) asset for Asset ID {child_asset_prop_ref}')
    
    if (len(no_asset_found_children) > 0):
        logger.log("Script completed. The following (child) assets could not be found:")
        for asset_id in no_asset_found_children:
            logger.log(asset_id)

    if (len(no_asset_found_parents) > 0):
        logger.log("Script completed. The following parent assets could not be found:")
        for asset_id in no_asset_found_parents:
            logger.log(asset_id)

    return update_count


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = Config.FILE_PATH
    asset_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER

    # Note: Batch write to update the asset data in dynamodb
    update_count = update_assets_with_parents_data(table, asset_csv_data, logger)
    logger.log(f"Updated {update_count} records")