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
    FILE_PATH = "aws/src/database/data/input/dwelling_hierarchy_upload_2.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }

def update_assets_with_parents_data(asset_table: Table, assets_from_csv: list[dict]) -> int:
    update_count = 0
    progress_bar = ProgressBar(len(assets_from_csv), bar_length=len(assets_from_csv) // 10)

    no_asset_found_children = []
    no_asset_found_parents = []

    for i, csv_asset_item in enumerate(assets_from_csv):        
        if i % 100 == 0:
            progress_bar.display(i)

        # Get AssetId/PropRef from CSV file
        child_asset_prop_ref = str(csv_asset_item["property_number"])

        # If AssetId is less than 8 digits, left pad it with 0s until AssetId is composed of 8 digits
        if len(child_asset_prop_ref) < 8:
            child_asset_prop_ref = child_asset_prop_ref.rjust(8, '0')

        # Get asset object, using the AssetId, from DynamoDb
        data_retrieve_child = get_by_secondary_index(asset_table, "AssetId", "assetId", child_asset_prop_ref)

        if (len(data_retrieve_child) > 0): 
            child_asset_record = data_retrieve_child[0]

            # NOW WE HAVE THE CHILD ASSET OBJECT

            # Get the parent from the CSV file for the asset
            parent_asset_prop_ref = str(csv_asset_item["parent"])

            # If the value is "Hackney" and not a valid AssetId, we use the Hackney Home GUID (656feda1-896f-b136-da84-163ee4f1be6c)
            if parent_asset_prop_ref == "Hackney":
                parent_asset_guid = "656feda1-896f-b136-da84-163ee4f1be6c"

            # Otherwise, if the value is not "Hackney", we need to find the record
            else:
                # If AssetId is less than 8 digits, left pad it with 0s until AssetId is composed of 8 digits
                    if len(parent_asset_prop_ref) < 8:
                        parent_asset_prop_ref = parent_asset_prop_ref.rjust(8, '0')

                    # Get asset object, using the AssetId, from DynamoDb, for the parent asset
                    data_retrieve_parent = get_by_secondary_index(asset_table, "AssetId", "assetId", parent_asset_prop_ref)

                    # If we have results, get the first object and get its GUID
                    if (len(data_retrieve_parent) > 0):
                        parent_asset_guid = data_retrieve_parent[0]['id']
                    # If not output error message and make note of the asset for which a parent CANNOT be found
                    else:
                        no_asset_found_parents.append(child_asset_prop_ref)
                        print('Cannot find (parent) asset for Asset ID', child_asset_prop_ref)

            # NOW WE HAVE THE GUID FOR THE PARENT ASSET (whether this is HackneyHomes or not)

            # Now we set "parentAssetIds" field of the child asset record to the value of parent_asset_guid
            child_asset_record["parentAssetIds"] = parent_asset_guid

            # SAVE THE EDITED ASSET RECORD
            asset_table.put_item(Item=child_asset_record)
            
            update_count += 1
        else:
            no_asset_found_children.append(child_asset_prop_ref)
            print('Cannot find (child) asset for Asset ID', child_asset_prop_ref)
            
    print("SCRIPT FINISHED. CANNOT FIND ASSET (CHILD)", no_asset_found_children)
    print("SCRIPT FINISHED. CANNOT FIND ASSET (PARENT)", no_asset_found_parents)
    return update_count


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = Config.FILE_PATH
    asset_csv_data = csv_to_dict_list(_file_path)

    logger = Config.LOGGER

    # Note: Batch write to update the asset data in dynamodb
    update_count = update_assets_with_parents_data(table, asset_csv_data)
    logger.log(f"Updated {update_count} records")