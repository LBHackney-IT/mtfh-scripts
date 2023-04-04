from dataclasses import dataclass
from typing import TextIO, Any

from aws.src.database.dynamodb.domain.domain_objects import Asset
from aws.src.database.dynamodb.utils import dynamodb_to_csv
from aws.src.utils.logger import Logger
from enums.enums import Stage


@dataclass
class Config:
    TABLE_NAME = "Assets"
    OUTPUT_CLASS = Asset
    LOGGER = Logger()
    STAGE = Stage.HOUSING_DEVELOPMENT
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

    # This works, but doesn't give any fine control, could write more generic scripts
    dynamodb_to_csv("Assets", Config.STAGE, Config.OUTPUT_DIRECTORY, Config.HEADINGS_FILTERS)
