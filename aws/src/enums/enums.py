from enum import Enum


class Stage(Enum):
    PRODUCTION = "production"
    STAGING = "staging"
    DEVELOPMENT = "development"


class PersonType:
    HOUSING_OFFICER = "HousingOfficer"
    HOUSING_AREA_MANAGER = "HousingAreaManager"
