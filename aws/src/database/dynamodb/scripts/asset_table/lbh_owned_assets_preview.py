from dataclasses import dataclass

from mypy_boto3_dynamodb.service_resource import Table

from aws.src.database.dynamodb.utils.get_by_secondary_index import get_by_secondary_index
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.csv_to_dict_list import csv_to_dict_list
from aws.src.utils.logger import Logger
from aws.src.utils.progress_bar import ProgressBar
from enums.enums import Stage

import csv

@dataclass
class Config:
    TABLE_NAME = "Assets"
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    FILE_PATH = "aws/src/database/data/input/Asset Ownership List - All Dwellings.csv"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }


def test_csv():
    output_csv_file_file_path = 'aws/src/database/data/output/output-test.csv'

    with open(output_csv_file_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        writer.writerow(['Name', 'Age', 'City'])
        writer.writerow(['John', 30, 'New York'])
        writer.writerow(['Alice', 25, 'Los Angeles'])
        writer.writerow(['Bob', 28, 'Chicago'])

def main():
    # table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    # _file_path = Config.FILE_PATH
    # asset_csv_data = csv_to_dict_list(_file_path)

    # logger = Config.LOGGER

    # # Note: Batch write to update the asset data in dynamodb
    # update_count = update_assets_with_parents_data(table, asset_csv_data, logger)
    # logger.log(f"Updated {update_count} records")
    test_csv()