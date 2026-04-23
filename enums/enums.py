from enum import Enum
from typing import Literal, cast

Environment = Literal["development", "staging", "production"]


class Stage(Enum):
    HOUSING_PRODUCTION = "housing-production"
    HOUSING_PRODUCTION_READONLY = "housing-production-readonly"
    HOUSING_STAGING = "housing-staging"
    HOUSING_DEVELOPMENT = "housing-development"
    HOUSING_DEVELOPMENT_READONLY = "housing-development-readonly"
    BASE_DEVELOPMENT = "base-development"
    BASE_STAGING = "base-staging"
    BASE_PRODUCTION = "base-production"
    DEVELOPMENT_APIS = "development-apis"
    STAGING_APIS = "staging-apis"
    PRODUCTION_APIS = "production-apis"
    DISASTER_RECOVERY = "disaster-recovery"

    @property
    def env_name(self) -> Environment:
        value = self.value
        for stage_str in ["development", "staging", "production"]:
            if stage_str in value:
                return cast(Environment, stage_str)
        raise ValueError(f"Stage {self.value} not recognised")

    def to_env_name(self) -> Environment:
        return self.env_name
