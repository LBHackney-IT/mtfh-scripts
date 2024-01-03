from dataclasses import dataclass

from aws.database.dynamodb.utils.dynamodb_to_csv import dynamodb_to_tsv
from enums.enums import Stage


@dataclass
class Config:
    STAGE = Stage.HOUSING_DEVELOPMENT
    OUTPUT_DIRECTORY = "../../output"
    HEADINGS_FILTERS = {
        "id": lambda x: bool(x),
        "firstName": lambda x: bool(x),
        "surname": lambda x: bool(x),
        "personTypes": lambda x: any(person_type in ["HousingAreaManager", "HousingOfficer"] for person_type in x),
    }


def main():
    dynamodb_to_tsv("Persons", Config.STAGE, Config.OUTPUT_DIRECTORY, Config.HEADINGS_FILTERS)
