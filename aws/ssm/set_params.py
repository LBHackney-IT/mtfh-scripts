from typing import TypedDict, cast
from mypy_boto3_ssm.client import SSMClient

from aws.authentication.generate_aws_resource import get_session_for_stage
from enums.enums import Stage


class Parameter(TypedDict):
    name: str
    value: str
    description: str


class ParamValue(TypedDict):
    client: SSMClient
    stage: Stage
    value: str


def create_param(param: ParamValue):
    parameter = Parameter(
        name=f"/housing-tl/{param['stage'].env_name}/disallowed-email",
        value=param["value"],
        description="User with this email is not allowed to access specific record(s) due to a conflict of interest - see HPT-641",
    )
    session = get_session_for_stage(param["stage"])
    ssm_client = cast(SSMClient, session.client("ssm"))
    ssm_client.put_parameter(
        Name=parameter["name"],
        Value=parameter["value"],
        Description=parameter["description"],
        Type="String",
        Overwrite=True,
    )


def check_param(param: ParamValue):
    ssm_client = cast(SSMClient, param["client"])
    response = ssm_client.get_parameter(
        Name=f"/housing-tl/{param['stage'].env_name}/disallowed-email"
    )
    value = response.get("Parameter", {}).get("Value")
    print(f"Value for {param['stage'].env_name} is {value}")


def main():
    param_values = [
        ParamValue(
            client=cast(SSMClient, get_session_for_stage(stage).client("ssm")),
            stage=stage,
            value=value,
        )
        for stage, value in [
            (Stage.HOUSING_DEVELOPMENT, "adam.tracy@hackney.gov.uk"),
            (Stage.HOUSING_STAGING, "adam.tracy@hackney.gov.uk"),
            (Stage.HOUSING_PRODUCTION, "farouk.mohamed@hackney.gov.uk"),
        ]
    ]
    for param_value in param_values:
        create_param(param_value)
        check_param(param_value)


if __name__ == "__main__":
    main()
