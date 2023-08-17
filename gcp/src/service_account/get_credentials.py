from mypy_boto3_ssm import SSMClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage


def get_credentials(stage = Stage.HOUSING_DEVELOPMENT, url_stage = None) -> dict:
    if url_stage is None:
        url_stage = stage.value.replace("housing-", "")
    ssm: SSMClient = generate_aws_service("ssm", stage, "client")
    path = f"/housing-finance/{url_stage}/google-application-credentials-json"
    credentials = ssm.get_parameter(Name=path)["Parameter"]["Value"]
    return credentials

