from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.utils.csv_to_dict_list import csv_to_dict_list
from aws.utils.logger import Logger
from aws.utils.progress_bar import ProgressBar
from enums.enums import Stage

import csv
import requests

# The below two imports require "pip install python-dotenv" and an .env file with the AUTH_TOKEN and API_ENDPOINT values
from dotenv import load_dotenv
import os

@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws/src/database/data/input/Asset Ownership List - Compact - All Dwellings.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }


# IMPORTANT: It is advised to open a specific terminal window prior to running this script,
# so that after the script has run, the terminal will not close, and you will be able to view
# the final output (displaying any assets that, for whatever reason, -could not be updated).

def generate_lbh_owned_assets_csv(asset_table: Table, assets_from_csv: list[dict], writer, logger: Logger) -> int:
    progress_bar = ProgressBar(len(assets_from_csv))

    assets_not_found = []
    assets_in_csv_file = []

    # Add columns with headers
    writer.writerow(['PropRef', 'AssetType', 'Address', 'Postcode'])

    for i, csv_asset_item in enumerate(assets_from_csv):
        if i % 100 == 0:
            progress_bar.display(i)

        # Get AssetId/PropRef from CSV file
        asset_prop_ref = str(csv_asset_item["PropRef"])
        print(f"Asset {asset_prop_ref}")

        # If AssetId is less than 8 digits, left pad it with 0s until AssetId is composed of 8 digits
        if len(asset_prop_ref ) < 8:
            asset_prop_ref  = asset_prop_ref .rjust(8, '0')

            # Get asset object, using the AssetId, from DynamoDb
            db_data_retrieve = get_by_secondary_index(asset_table, "AssetId", "assetId", asset_prop_ref)

            if (len(db_data_retrieve) > 0):
                # If we successfully retrieve data, we can access the asset object
                asset_db = db_data_retrieve[0]

                # Set endpoint URL for retrieving children assets
                endpoint_url = os.getenv("API_ENDPOINT")

                # Set auth token
                authorization_token = os.getenv("AUTH_TOKEN")

                # Set API call parameters
                params = {
                    "searchText": asset_db["id"],
                    "pageSize": 1000
                }

                # Call API and retrieve children assets
                api_response = call_api(endpoint_url, params, authorization_token, logger)
                children_assets = api_response["childAssets"]

                # ADD CURRENT ASSET TO CSV
                # print(f"Adding current asset {asset_db['assetId']}")
                add_asset_to_csv(assets_in_csv_file, asset_db, writer, logger, "CURRENT")

                # ADD PARENT ASSET TO CSV (if we have parent data)
                if (asset_db['assetLocation'] and len(asset_db['assetLocation']['parentAssets']) > 0):

                    parent_asset_db = asset_table.get_item(Key={"id": asset_db['assetLocation']['parentAssets'][0]['id']})
                    parent_asset_db = parent_asset_db.get("Item")

                    if parent_asset_db is None:
                        logger.log(f"Cannot find parent asset {asset_db['assetLocation']['parentAssets'][0]['id']} for Asset ID {asset_prop_ref}")
                    else:
                        # print(f"Adding parent of current asset {asset_db['assetId']} with PropRef {parent_asset_db['assetId']}")
                        add_asset_to_csv(assets_in_csv_file, parent_asset_db, writer, logger, "PARENT")

                if len(children_assets) > 0:
                    # ADD CHILDREN ASSETS TO CSV
                    for child_asset in children_assets:
                        # print(f"Adding child of current asset {asset_db['assetId']} with PropRef {child_asset['assetId']}")
                        add_asset_to_csv(assets_in_csv_file, child_asset, writer, logger, "CHILD")
                # else:
                #     print(f"No children found for current asset id {asset_db['assetId']}")

            else:
                assets_not_found.append(asset_prop_ref)
                logger.log(f'Cannot find asset for Asset ID {asset_prop_ref}')

    return assets_not_found, len(assets_in_csv_file)

def call_api(endpoint_url: str, params: dict, authorization_token: str, logger: Logger) -> dict:
    try:
        headers = {
            "Authorization": f"Bearer {authorization_token}",
        }

        response = requests.get(endpoint_url, params=params, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            logger.log(f"API request failed with status code {response.status_code}")
            return None

    except Exception as e:
        logger.log(f"An error occurred while making the API request: {str(e)}")
        return None

def add_asset_to_csv (assets_in_csv_file, asset, writer, logger, asset_relationship):

    # # Check if asset is already in CSV file
    if asset['assetId'] not in assets_in_csv_file:

        # If no, add it
        writer.writerow([asset['assetId'], asset['assetType'], asset['assetAddress']['addressLine1'], asset['assetAddress']['postCode']])
        assets_in_csv_file.append(asset['assetId'])

    # If yes, do nothing
    else:
        return
        # logger.log(f"PropRef {asset['assetId']} of {asset_relationship} is already in the CSV, skipping asset.")

def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    _file_path = Config.FILE_PATH
    asset_csv_data = csv_to_dict_list(_file_path)
    load_dotenv()
    logger = Config.LOGGER
    output_csv_file_file_path = 'aws/src/database/data/output/output-test.csv'

    # Create/reference CSV file
    with open(output_csv_file_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        assets_not_found, assets_in_csv_file = generate_lbh_owned_assets_csv(table, asset_csv_data, writer, logger)

    if len(assets_not_found) > 0:
            logger.log("The following assets could not be found in the database", assets_not_found)

    logger.log(f"Number of assets added to CSV file: {str(assets_in_csv_file)}")
