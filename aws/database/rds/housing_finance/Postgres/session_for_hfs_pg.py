"""
Note: Ensure to install unixodbc to be able to use pyodbc
"""

import os
from sqlalchemy import create_engine

from aws.database.rds.housing_finance.Postgres.entities.GoogleFileSetting import (
    Base as HousingFinanceBase,
    GoogleFileSetting,
)
from mypy_boto3_ssm import SSMClient
from sqlalchemy.orm import sessionmaker, Session as SA_Session
from psycopg2 import connect as psycopg2_connect

from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage


def session_for_hfs(
    stage: Stage, expire_on_commit=True, local_port=5432
) -> sessionmaker[SA_Session]:
    """
    Connect to cautionary alerts database
    :param stage: Stage to connect to
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """
    assert isinstance(stage, Stage), f"stage must be of type Stage, not {type(stage)}"

    pg_username_path = f"/housing-finance/{stage.to_env_name()}/db-username"
    pg_password_path = f"/housing-finance/{stage.to_env_name()}/db-password"
    db_name_path = f"/housing-finance/{stage.to_env_name()}/db-database"

    ssm: SSMClient = generate_aws_service("ssm", stage)
    username = ssm.get_parameter(Name=pg_username_path)["Parameter"].get("Value")
    password = ssm.get_parameter(Name=pg_password_path)["Parameter"].get("Value")
    db_name = ssm.get_parameter(Name=db_name_path)["Parameter"].get("Value", "").lower()
    assert username and password and db_name, "Ensure all parameters are set in SSM"

    connection_string = (
        f"postgresql://{username}:{password}@localhost:{local_port}/sow2b"
    )
    print(f"Connecting to {connection_string}")
    engine = create_engine(
        "postgresql+psycopg2://",
        creator=lambda: psycopg2_connect(connection_string),
        echo=True,
    )
    HousingFinanceBase.metadata.create_all(bind=engine, tables=[])

    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session


if __name__ == "__main__":
    """
    Example usage
    """
    HfsSession = session_for_hfs(Stage.HOUSING_DEVELOPMENT)
    with HfsSession.begin() as session:
        res = (
            session.query(GoogleFileSetting)
            .where(GoogleFileSetting.label == "TenancyAgreement")
            .all()
        )
        print(res)
