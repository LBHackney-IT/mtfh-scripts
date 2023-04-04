from dataclasses import dataclass

from aws.src.database.dynamodb.domain.domain_objects import Asset
from aws.src.database.dynamodb.utils.dynamodb_to_csv import dynamodb_to_csv
from aws.src.utils.logger import Logger
from enums.enums import Stage


@dataclass
class Config:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
    OUTPUT_DIRECTORY = "../../output"
    
    # Put all headings / keys you want to extract from the DynamoDB table here
    HEADING_FILTERS = {
        "id": lambda x: bool(x),
        "assetId": lambda x: bool(x),
        "patches": lambda x: bool(x),
    }


logger = Logger()

if __name__ == "__main__":
    with open(Config.OUTPUT_DIRECTORY, "w") as outfile:
        outfile.write("")

    # This works, but doesn't give any fine control, could write more generic scripts
    dynamodb_to_csv("Assets", Config.STAGE, Config.OUTPUT_DIRECTORY, Config.HEADING_FILTERS)
