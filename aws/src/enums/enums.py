from enum import Enum


class Stage(Enum):
    PRODUCTION = HOUSING_PRODUCTION = "housing-production"
    STAGING = HOUSING_STAGING = "housing-staging"
    DEVELOPMENT = HOUSING_DEVELOPMENT = "housing-development"
    BASE_DEVELOPMENT = "base-development"
    BASE_STAGING = "base-staging"
    BASE_PRODUCTION = "base-production"


class PersonType:
    HOUSING_OFFICER = "HousingOfficer"
    HOUSING_AREA_MANAGER = "HousingAreaManager"
