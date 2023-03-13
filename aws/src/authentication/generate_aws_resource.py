from typing import Literal, Any
import boto3
import botocore.exceptions
from boto3 import Session
from boto3.resources.base import ServiceResource
from aws.src.enums.enums import Stage


def get_session_for_stage(stage: Stage) -> Session:
    """
    Set up your credentials in ~/.aws/credentials before use \n
    You can do this with the aws cli by running `aws configure --profile <stage>`
    You may need to manually modify the session token in this file when it expires
    :param stage: one of the stages in the Stage enum
    :return: the credentials for the given stage
    """

    # Get and validate credentials
    while True:
        try:
            session = boto3.Session(profile_name=stage.value.lower())
            sts_client = session.client("sts")
            identity = sts_client.get_caller_identity()
            print(f"Stage / AWS Profile: [{stage.value.lower()}]")
            print(f"Account ID: {identity['Account']}")
            print(f"Assumed Role: {identity['Arn'].split('/')[1]}")
            print(f"User: {identity['Arn'].split('/')[2]}")
            break
        except botocore.exceptions.ProfileNotFound:
            input(f"Couldn't find stage {stage.value.lower()} in your awscli credentials file.\n"
                  f">> Please run `aws configure --profile {stage.value.lower()}` to set up your credentials."
                  f"Hit enter to try again")
        except botocore.exceptions.ClientError:
            input(f"\nInvalid Access key ID / Secret access key / Session token for profile {stage.value.lower()}.\n"
                  f"Update the credentials in your .aws/credentials file for the {stage.value.lower()} profile.\n"
                  f"Hit enter to try again")
    return session


def generate_aws_resource(service_name: str, stage: Stage, service_type="resource") -> Any:
    """
    :param service_name: The name of the service to get, e.g. "dynamodb"
    :param stage: The stage to get credentials for
    :param service_type: The type of service to get, either "resource" or "client"
    :return: The service object
    """

    session: Session = get_session_for_stage(stage)
    match service_type:
        case "resource":
            valid_services = ["dynamodb", "s3", "sqs", "sns", "ssm", "sts"]
            if service_name not in valid_services:
                raise ValueError(f"Service name must be one of {valid_services}")
            # noinspection PyTypeChecker
            service: ServiceResource = session.resource(service_name)
        case "client":
            valid_services = ["ssm", "rds"]
            if service_name not in valid_services:
                raise ValueError(f"Service name must be one of {valid_services}")
            # noinspection PyTypeChecker
            service: ServiceResource = session.client(service_name)
        case _:
            raise ValueError(f"Type must be one of ['resource', 'client']")
    return service

if __name__ == "__main__":
    # Example usage + For testing
    generate_aws_resource("dynamodb", Stage.BASE_STAGING, "resource")
