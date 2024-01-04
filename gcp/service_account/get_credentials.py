"""Function to get GCP credentials"""
from gcp.src.service_account.utils.confirm import confirm
from mypy_boto3_ssm import SSMClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage


def get_credentials(stage = Stage.HOUSING_DEVELOPMENT, url_stage: str = None) -> dict:
    """
    Gets AWS credentials from parameter store for the Google Service Account
    :param stage: Stage / AWS Profile with the GCP credentials in parameter store
    :param url_stage: Stage string to use in the parameter name (path)
    :return: GCP Service Account credentials
    """
    if url_stage is None:
        url_stage = stage.value.replace("housing-", "")
    ssm: SSMClient = generate_aws_service("ssm", stage)
    path = f"/housing-finance/{url_stage}/google-application-credentials-json"
    credentials = ssm.get_parameter(Name=path)["Parameter"]["Value"]
    return credentials
