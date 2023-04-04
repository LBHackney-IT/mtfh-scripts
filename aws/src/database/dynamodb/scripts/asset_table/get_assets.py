import csv
from dataclasses import dataclass

from aws.src.database.dynamodb.domain.domain_objects import Asset
from aws.src.database.dynamodb.utils.dynamodb_item_factory import DynamodbItemFactory
from aws.src.database.dynamodb.utils.get_dynamodb_table import get_dynamodb_table
from aws.src.utils.logger import Logger
from enums.enums import Stage


@dataclass
class Config:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.BASE_DEVELOPMENT
    OUTPUT_DIRECTORY = "../../output"
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }


if __name__ == "__main__":
    with open(Config.OUTPUT_DIRECTORY, "w") as outfile:
        outfile.write("")
        writer = csv.writer(outfile)
        table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
        dynamodb_item_factory = DynamodbItemFactory(table, Config.OUTPUT_CLASS, Config.LOGGER)
        assets = dynamodb_item_factory.full_extract(Config.HEADING_FILTERS)

        # Script here to parse assets (including nested JSON) and write to csv
