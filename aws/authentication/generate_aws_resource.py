import subprocess
import boto3
import botocore.exceptions
from boto3 import Session
from boto3.resources.base import ServiceResource
from typing import Any, Literal

from enums.enums import Stage


def get_session_for_stage(stage: Stage | str) -> Session:
    if isinstance(stage, Stage):
        stage_profile = stage.value.lower()
    else:
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
            input(
                f"Couldn't find stage {stage_profile} in your awscli credentials file.\n"
                f">> Please run `aws configure sso` and set up profile {stage_profile} to set up your credentials."
                f"Hit enter to try again"
            )
        except botocore.exceptions.ClientError:
            # Thrown when profile manually configured in ~/.aws/credentials
            input(
                f"\nInvalid Access key ID / Secret access key / Session token for profile {stage_profile}.\n"
                f"Update the credentials in your .aws/credentials file for the {stage_profile} profile.\n"
                f"Hit enter to try again"
            )
        except botocore.exceptions.TokenRetrievalError:
            # Thrown when using SSO and credentials have expired
            input(
                f"\nInvalid or expired credentials for profile {stage_profile}.\n"
                f"aws sso login --profile {stage_profile}; || - Run to refresh.\n"
                f"If you have a {stage_profile} AWS SSO profile, hit enter to run this command and log in:"
            )
            subprocess.Popen(["aws", "sso", "login", "--profile", stage_profile]).wait()
            input("\nHit enter to try running the script again")
        except botocore.exceptions.NoRegionError:
            # Thrown when there is no region configured for the profile
            input(
                f"\nNo region configured for profile {stage_profile}.\n"
                f"Set a default region in your .aws/config file for the {stage_profile} profile.\n"
                f"Hit enter to try again"
            )
    return session


ServiceName = Literal["dynamodb", "ssm", "rds", "es", "opensearch", "logs", "sqs"]


def generate_aws_service(service_name: ServiceName, stage: Stage) -> Any:
    """
    :param service_name: The name of the service to get, e.g. "dynamodb"
    :param stage: The stage to get credentials for
    :return: The service object
    """
    session: Session = get_session_for_stage(stage)

    # Info on resources: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html
    # Info on clients: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html
    valid_resources: list[ServiceName] = ["dynamodb"]
    valid_clients: list[ServiceName] = [
        s for s in ServiceName.__args__ if s not in valid_resources
    ]
    if service_name in valid_resources:
        service: ServiceResource = session.resource(service_name)  # type: ignore
    elif service_name in valid_clients:
        service: ServiceResource = session.client(service_name)  # type: ignore
    else:
        raise ValueError(f"Valid service names are {valid_resources + valid_clients}")
    return service


if __name__ == "__main__":
    # Example usage + For testing
    generate_aws_service("dynamodb", Stage.BASE_STAGING)
