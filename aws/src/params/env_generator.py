"""
Generates the environment variables for an application based on app config text via parameter store fetching
How to use:
    - Set the PROFILE variable to the AWS profile you want to use
    - Set the STAGE variable to the stage you want to use
    - Run the script
    - Paste the param section from CircleCI / Serverless / Terraform
    - The environment variables will be printed to the console in env format
"""
from enum import Enum

from mypy_boto3_ssm import SSMClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage
import re

ssm_client: SSMClient = generate_aws_service("ssm", Stage.HOUSING_STAGING, "client")

PROFILE = "housing-staging"
STAGE = PROFILE.replace("housing-", "")


class SectionType(Enum):
    CIRCLE_CI = "CIRCLE_CI"
    SERVERLESS = "SERVERLESS"
    TERRAFORM = "TERRAFORM"


def env_generator(param_section: str) -> str:
    if any(["export " in param_line for param_line in param_section.split("\n")]):
        res = handle_param_section(param_section, SectionType.CIRCLE_CI)
    elif any(["${ssm:" in param_line for param_line in param_section.split("\n")]):
        res = handle_param_section(param_section, SectionType.SERVERLESS)
    elif any(["aws_ssm_parameter" in param_line for param_line in param_section.split("\n")]):
        res = handle_param_section(param_section, SectionType.TERRAFORM)
    else:
        raise Exception(f"Unknown param section format: {param_section}")

    print(res)
    return res


def handle_param_section(param_section: str, param_section_type: SectionType) -> str:
    if param_section_type == SectionType.CIRCLE_CI:
        stage_placeholder = "<<parameters.stage>>"
        key_regex = r'export ([^=]+)='
        path_regex = r'--name ([A-z|/]+)'
    elif param_section_type == SectionType.SERVERLESS:
        stage_placeholder = "${self:provider.stage}"
        key_regex = r'([A-z_0-9]+): \$'
        path_regex = r'ssm:([A-z|/]+)'
    elif param_section_type == SectionType.TERRAFORM:
        stage_placeholder = "${var.environment_name}"
        key_regex = r'data "aws_ssm_parameter" "([^"]+)" {[^}]+}'
        path_regex = r'name = "([^"]+)"'
    else:
        raise Exception(f"Unknown param section type: {param_section_type}")

    out_lines = []
    param_section = param_section.replace(stage_placeholder, STAGE)

    keys = re.findall(key_regex, param_section)
    paths = re.findall(path_regex, param_section)
    if not keys or not paths or len(keys) != len(paths):
        raise Exception(f"Error parsing param section: {param_section}")
    keys = [key.strip().upper() for key in keys]

    for i, path in enumerate(paths):
        try:
            param_value = ssm_client.get_parameter(Name=path, WithDecryption=True)['Parameter']['Value']
        except ssm_client.exceptions.ParameterNotFound:
            print(f"Parameter not found: {path} in {PROFILE}")
            continue
        out_lines.append(f'{keys[i]}="{param_value}"')

    return "\n".join(out_lines)


def main():
    param_text = input("Paste the param section from CircleCI / Serverless / Terraform: ")
    print("\n\nGenerating environment variables...")
    env_generator(param_text)


if __name__ == "__main__":
    main()
