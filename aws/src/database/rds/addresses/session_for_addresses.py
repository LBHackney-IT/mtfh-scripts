"""
Note: Ensure to install unixodbc to be able to use pyodbc
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SA_Session
from psycopg2 import connect as psycopg2_connect

from mypy_boto3_ssm import SSMClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

from entities.HackneyAddress import HackneyAddress, Base as HackneyAddressBase


def session_for_addresses(stage: Stage, expire_on_commit=True, local_port=1433) -> sessionmaker[SA_Session]:
    """
    Connect to cautionary alerts database
    :param stage: Stage to connect to
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """

    pg_username_path = f"/addresses-api/{stage.to_env_name()}/postgres-username"
    pg_password_path = f"/addresses-api/{stage.to_env_name()}/postgres-password"

    ssm: SSMClient = generate_aws_service('ssm', stage, 'client')
    username = ssm.get_parameter(Name=pg_username_path)['Parameter']['Value']
    password = ssm.get_parameter(Name=pg_password_path)['Parameter']['Value']

    connection_string = f"postgresql://{username}:{password}@localhost:{local_port}/addresses_api"
    engine = create_engine("postgresql+psycopg2://", creator=lambda: psycopg2_connect(connection_string), echo=True)
    HackneyAddressBase.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session


if __name__ == "__main__":
    """
    Example usage
    """
    AddressSession = session_for_addresses(Stage.BASE_STAGING)
    with AddressSession.begin() as session:
        res = session.query(HackneyAddress).where(HackneyAddress.uprn == "100021021442").all()
        print(res[0])
