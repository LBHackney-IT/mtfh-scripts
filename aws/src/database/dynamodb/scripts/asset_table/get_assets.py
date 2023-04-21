from dataclasses import dataclass
from typing import get_type_hints

from aws.src.database.domain.dynamo_domain_objects import Asset
from aws.src.database.dynamodb.utils.dynamodb_item_factory import DynamodbItemFactory
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.logger import Logger
from enums.enums import Stage


@dataclass
class Config:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }
    ITEM_COUNT_LIMIT = 100  # Set to None to return all items


def main():
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    dynamodb_item_factory = DynamodbItemFactory(table, Config.OUTPUT_CLASS, Config.LOGGER)
    assets = dynamodb_item_factory.full_extract(Config.HEADING_FILTERS, item_limit=Config.ITEM_COUNT_LIMIT)

    # Optionally write script here to parse asset objects, see domain/dynamo_domain_objects.py for the Asset class

    # Note: Writing to TSV which can be imported into Google Sheets
    with open("assets.tsv", "w") as f:
        headings = get_type_hints(Config.OUTPUT_CLASS).keys()
        f.write("\t".join(headings) + "\n")
        for asset in assets:
            f.write("\t".join([str(asset.__dict__[heading]) for heading in headings]) + "\n")
