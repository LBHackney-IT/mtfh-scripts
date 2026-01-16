"""
Script to copy SSM parameters between AWS accounts.
Can be used for disaster recovery or other migrations.
Edit the STAGE_SOURCE and STAGE_TARGET variables to set source and target accounts.
Edit the OPERATION_TYPE variable to choose between copying by prefix or by name,
  then set the relevant values for PARAMETER_BY_PREFIX_PREFIX or PARAMETER_BY_NAME_NAMES.
"""

from typing_extensions import Literal
from mypy_boto3_ssm.client import SSMClient
from mypy_boto3_ssm.type_defs import ParameterTypeDef

from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage
from utils.confirm import confirm

# ==== Configuration ====
# Edit these to set source and target accounts
STAGE_SOURCE = Stage.HOUSING_PRODUCTION
STAGE_TARGET = Stage.DISASTER_RECOVERY

# Set operation type: "by_prefix" or "by_name"
OPERATION_TYPE: Literal["by_prefix", "by_name"] = "by_prefix"

# If OPERATION_TYPE is "by_prefix", set this prefix
#   to copy all parameters starting with this prefix
PARAMETER_BY_PREFIX_PREFIX = "/prefix-goes/here/"

# If OPERATION_TYPE is "by_name", set these parameter names
#   to copy specific parameters with these names
PARAMETER_BY_NAME_NAMES = [
    "/parameter-paths/go/here",
    "/another-parameter-path",
]
# =======================

# Create ssm clients for source and target accounts
ssm_source: SSMClient = generate_aws_service("ssm", STAGE_SOURCE)
ssm_target: SSMClient = generate_aws_service("ssm", STAGE_TARGET)

ENVIRONMENT = STAGE_SOURCE.to_env_name()


def get_params_with_prefix(ssm_service: SSMClient, prefix: str):
    """Fetch SSM parameters by prefix from the given SSM service"""
    parameters: list[ParameterTypeDef] = []
    paginator = ssm_service.get_paginator("get_parameters_by_path")
    for page in paginator.paginate(Path=prefix, Recursive=True, WithDecryption=True):
        parameters.extend(page["Parameters"])
    return parameters


def get_ssm_parameters(ssm_service: SSMClient, parameter_names: list[str]):
    """Fetch SSM parameters by name from the given SSM service"""
    parameters: list[ParameterTypeDef] = []
    for param_path in parameter_names:
        response = ssm_service.get_parameter(Name=param_path, WithDecryption=True)
        parameters.append(response["Parameter"])
        assert "Name" in response["Parameter"]
        assert response["Parameter"]["Name"] == param_path
    return parameters


def migrate_parameters(parameters: list[ParameterTypeDef]):
    """Migrate SSM parameters from source to target account"""
    for parameter in parameters:
        assert "Name" in parameter, f"Parameter missing Name: {parameter}"
        assert "Value" in parameter, f"Parameter missing Value: {parameter}"
        assert "Type" in parameter, f"Parameter missing Type: {parameter}"
        print(parameter["Name"])

        # Check if parameter already exists in target account - skip if values match
        try:
            existing_param = ssm_target.get_parameter(
                Name=parameter["Name"], WithDecryption=True
            ).get("Parameter")
            if existing_param and "Value" in existing_param:
                print(f"Param {parameter['Name']} already: {existing_param['Value']}")
                if existing_param["Value"] == parameter["Value"]:
                    print("Values match, skipping...")
                    continue
                else:
                    print("Values differ.")
        except ssm_target.exceptions.ParameterNotFound:
            print("Parameter not found in target account, proceeding to create.")
            pass

        # Ask for confirmation before overwriting
        if not confirm(f"Copy parameter {parameter['Name']} to target account?"):
            print("Skipping...")
            continue
        ssm_target.put_parameter(
            Name=parameter["Name"],
            Description=parameter.get("Description", ""),
            Value=parameter["Value"],
            Type=parameter["Type"],
            Overwrite=True,
        )


def check_paramters_created(parameters: list[ParameterTypeDef]):
    """Fetch all ssm parameters and print in the form NAME_APP_URL=value"""

    def param_path_to_name(param_path: str) -> str:
        """/housing-tl/production/activities-app-url -> ACTIVITIES_APP_URL"""
        return param_path.split("/")[-1].replace("-", "_").upper()

    for parameter in parameters:
        assert "Name" in parameter, f"Parameter missing Name: {parameter}"
        assert "Value" in parameter, f"Parameter missing Value: {parameter}"
        print(f"{param_path_to_name(parameter['Name'])}={parameter['Value']}")


def main():
    def fetch_parameters(ssm_client: SSMClient):
        if OPERATION_TYPE == "by_name":
            parameters = get_ssm_parameters(ssm_client, PARAMETER_BY_NAME_NAMES)
        elif OPERATION_TYPE == "by_prefix":
            parameters = get_params_with_prefix(ssm_client, PARAMETER_BY_PREFIX_PREFIX)
        else:
            raise ValueError(f"Unknown OPERATION_TYPE: {OPERATION_TYPE}")
        return parameters

    # Fetch and migrate parameters from the source account
    source_parameters = fetch_parameters(ssm_source)
    migrate_parameters(source_parameters)

    # Fetch and print parameters from the target account
    target_parameters = fetch_parameters(ssm_target)
    check_paramters_created(target_parameters)


if __name__ == "__main__":
    main()
