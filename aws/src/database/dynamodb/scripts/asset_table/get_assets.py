from dataclasses import dataclass
from typing import TextIO, Any

from aws.src.database.domain.domain_objects import Asset
from aws.src.database.utils.dynamodb_item_factory import DynamodbItemFactory
from aws.src.database.utils.dynamodb_to_csv import dynamodb_to_csv
from aws.src.enums.enums import Stage
from aws.src.database.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.logger import Logger


@dataclass
class Config:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.DEVELOPMENT
    OUTPUT_DIRECTORY = "../../output"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
        "assetId": lambda x: bool(x),
        "patches": lambda x: bool(x),
    }


logger = Logger()


def write_database_items_to_csv(items: list[Any], outfile: TextIO, headings: list[str]):
    """
    :param items: Items to write
    :param outfile: CSV file to write to
    :param headings: List of item properties (keys) to write to file as headings, e.g. ["id", "assetId", "patches"]
    :return:
    """
    outfile.write(",".join(Config.HEADING_FILTERS.keys()))
    csv_rows = [",".join(f",{obj[heading]}" for heading in headings) for obj in items]
    outfile.writelines(csv_rows)


if __name__ == "__main__":
    with open(Config.OUTPUT_DIRECTORY, "w") as outfile:
        outfile.write("")

    # TODO: Need a way to write assets to csv, or need method to return JSON from DynamoDB?
    # table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    # dynamodb_item_factory = DynamodbItemFactory(table, Config.OUTPUT_CLASS, Config.LOGGER)
    # assets = dynamodb_item_factory.full_extract(Config.HEADING_FILTERS)
    # with open(Config.OUTPUT_DIRECTORY, "w") as outfile:
    #     write_database_items_to_csv(assets, outfile, list(Config.HEADING_FILTERS.keys()))

    # This works, but doesn't give any fine control
    dynamodb_to_csv("Assets", Config.STAGE, Config.OUTPUT_DIRECTORY, Config.HEADINGS_FILTERS)
