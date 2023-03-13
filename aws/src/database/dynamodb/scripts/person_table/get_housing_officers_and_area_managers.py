from dataclasses import dataclass

from src.database.utils.dynamodb_to_csv import dynamodb_to_csv
from src.enums.enums import Stage
from src.utils.logger import Logger


@dataclass
class Config:
    STAGE = Stage.DEVELOPMENT
    OUTPUT_DIRECTORY = "../../output"
    HEADINGS_FILTERS = {
        "id": lambda x: bool(x),
        "firstName": lambda x: bool(x),
        "surname": lambda x: bool(x),
        "personTypes": lambda x: any(person_type in ["HousingAreaManager", "HousingOfficer"] for person_type in x),
    }


logger = Logger()

if __name__ == "__main__":
    dynamodb_to_csv("Persons", Config.STAGE, Config.OUTPUT_DIRECTORY, Config.HEADINGS_FILTERS)
