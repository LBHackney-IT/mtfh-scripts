import boto3
import botocore.exceptions
from boto3 import Session
from boto3.resources.base import ServiceResource
from typing import Any

from enums.enums import Stage


def get_session_for_stage(stage: Stage | str) -> Session:
    try:
        stage_profile = stage.value.lower()
    except AttributeError:
        stage_profile = stage.lower()

    # Get and validate credentials
    while True:
        try:
            session = boto3.Session(profile_name=stage_profile)
            sts_client = session.client("sts")
            identity = sts_client.get_caller_identity()
            print(f"Stage / AWS Profile: [{stage_profile}]")
            print(f"Account ID: {identity['Account']}")
            print(f"Assumed Role: {identity['Arn'].split('/')[1]}")
            print(f"User: {identity['Arn'].split('/')[2]}")
            break
        except botocore.exceptions.ProfileNotFound:
            input(f"Couldn't find stage {stage_profile} in your awscli credentials file.\n"
                  f">> Please run `aws configure sso` and set up profile {stage_profile} to set up your credentials."
                  f"Hit enter to try again")
        except botocore.exceptions.ClientError:
            # Thrown when profile manually configured in ~/.aws/credentials
            input(f"\nInvalid Access key ID / Secret access key / Session token for profile {stage.value.lower()}.\n"
                  f"Update the credentials in your .aws/credentials file for the {stage.value.lower()} profile.\n"
                  f"Hit enter to try again")
        except botocore.exceptions.TokenRetrievalError:
            # Thrown when using SSO and credentials have expired
            input(f"\nInvalid or expired credentials for profile {stage_profile}.\n"
                  f"aws sso login --profile {stage_profile}; || - Run to refresh.\n"
                  f"Hit enter to try again")
        except botocore.exceptions.NoRegionError:
            # Thrown when there is no region configured for the profile
            input(f"\nNo region configured for profile {stage_profile}.\n"
                  f"Set a default region in your .aws/config file for the {stage_profile} profile.\n"
                  f"Hit enter to try again")
    return session


def generate_aws_service(service_name: str, stage: Stage, service_type="resource") -> Any:
    """
    :param service_name: The name of the service to get, e.g. "dynamodb"
    :param stage: The stage to get credentials for
    :param service_type: The type of service to get, either "resource" or "client"
    :return: The service object
    """

    session: Session = get_session_for_stage(stage)
    match service_type:
        case "resource":
            # Add to these as needed
            valid_services = ["dynamodb"]
            if service_name not in valid_services:
                raise ValueError(f"Service name must be one of {valid_services}")
            # noinspection PyTypeChecker
            service: ServiceResource = session.resource(service_name)
        case "client":
            # Add to these as needed
            valid_services = ["ssm", "rds", "es", "ec2"]
            if service_name not in valid_services:
                raise ValueError(f"Service name must be one of {valid_services}")
            # noinspection PyTypeChecker
            service: ServiceResource = session.client(service_name)
        case _:
            raise ValueError(f"Type must be one of ['resource', 'client']")
    return service


if __name__ == "__main__":
    # Example usage + For testing
    generate_aws_service("dynamodb", Stage.BASE_STAGING, "resource")
