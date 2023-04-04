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
    STAGE = Stage.HOUSING_DEVELOPMENT
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
    }


if __name__ == "__main__":
    table = get_dynamodb_table(Config.TABLE_NAME, Config.STAGE)
    dynamodb_item_factory = DynamodbItemFactory(table, Config.OUTPUT_CLASS, Config.LOGGER)
    assets = dynamodb_item_factory.full_extract(Config.HEADING_FILTERS)

    # Script here to parse asset objects, see domain/domain_objects.py for the Asset class
