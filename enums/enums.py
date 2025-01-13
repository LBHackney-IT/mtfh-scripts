from enum import Enum


class Stage(Enum):
    HOUSING_PRODUCTION = "housing-production"
    HOUSING_PRODUCTION_READONLY = "housing-production-readonly"
    HOUSING_STAGING = "housing-staging"
    HOUSING_DEVELOPMENT = "housing-development"
    BASE_DEVELOPMENT = "base-development"
    BASE_STAGING = "base-staging"
    BASE_PRODUCTION = "base-production"

    def to_env_name(self) -> str:
        value = self.value
        for stage_str in ["development", "staging", "production"]:
            if stage_str in value:
                return stage_str
        raise ValueError(f"Stage {self.value} not recognised")
