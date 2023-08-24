from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage


def set_param(stage: Stage, param_name: str, param_value: str, description: str):
    """
    Sets a parameter in each stage of the AWS Parameter Store
    :param stage: The stage to set the parameter in
    :param param_name: The name of the parameter to set
    :param param_value: The value of the parameter to set
    :param description: The description of the parameter to set
    :return: None
    """
    ssm_client = generate_aws_service("ssm", stage, "client")
    ssm_client.put_parameter(
        Name=param_name,
        Value=param_value,
        Description=description,
        Type="String",
        Overwrite=True
    )

def main():
    """
    Sets parameters in each stage of the AWS Parameter Store
    :return: None
    """
    for stage in [Stage.HOUSING_DEVELOPMENT, Stage.HOUSING_STAGING, Stage.HOUSING_PRODUCTION]:
        url_stage = stage.value.replace("housing-", "")
        param_value = "mmh-project-team, e2e-testing-development, e2e-testing-staging, e2e-testing-production, mmh-tenure-enhanced-edit-access"
        description = "A comma-separated list of google groups to give access to edit inactive tenure start/end dates and active tenure start dates"
        set_param(stage, f"/housing-tl/{url_stage}/mmh-edit-tenure-groups-admin", param_value, description)
    