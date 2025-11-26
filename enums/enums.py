from enum import Enum
from typing import Literal, cast


Environment = Literal["development", "staging", "production"]


class Stage(Enum):
    HOUSING_PRODUCTION = "housing-production"
    HOUSING_STAGING = "housing-staging"
    HOUSING_DEVELOPMENT = "housing-development"
    BASE_DEVELOPMENT = "base-development"
    BASE_STAGING = "base-staging"
    BASE_PRODUCTION = "base-production"
    DISASTER_RECOVERY = "disaster-recovery"

    def to_env_name(self) -> Environment:
        value = self.value
        for stage_str in ["development", "staging", "production"]:
            if stage_str in value:
                return cast(Environment, stage_str)
        raise ValueError(f"Stage {self.value} not recognised")
