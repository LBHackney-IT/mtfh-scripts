"""
Note: Ensure to install unixodbc to be able to use pyodbc
"""

import pyodbc
from sqlalchemy import create_engine

from aws.database.rds.housing_finance.entities.GoogleFileSetting import Base as HousingFinanceBase
from mypy_boto3_ssm import SSMClient
from sqlalchemy.orm import sessionmaker, Session as SA_Session

from aws.authentication.generate_aws_resource import generate_aws_service
from aws.database.rds.housing_finance.entities.GoogleFileSetting import GoogleFileSetting
from enums.enums import Stage


def session_for_hfs(stage: Stage, expire_on_commit=True, local_port=1433) -> sessionmaker[SA_Session]:
    """
    Connect to cautionary alerts database
    :param stage: Stage to connect to
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """
    assert isinstance(stage, Stage), f"stage must be of type Stage, not {type(stage)}"

    pg_username_path = f"/housing-finance/{stage.to_env_name()}/db-username"
    pg_password_path = f"/housing-finance/{stage.to_env_name()}/db-password"

    ssm: SSMClient = generate_aws_service('ssm', stage, 'client')
    username = ssm.get_parameter(Name=pg_username_path)['Parameter']['Value']
    password = ssm.get_parameter(Name=pg_password_path)['Parameter']['Value']

    try:
        # Find first locally installed driver with Sql Server in the name
        driver = [driver for driver in pyodbc.drivers() if "SQL Server" in driver][0]
    except IndexError:
        raise IndexError("No ODBC Driver 17 for SQL Server found. Please install the driver and try again.")

    connection_string = f"DRIVER={driver};SERVER=127.0.0.1,{local_port};DATABASE=sow2b;UID={username};PWD={password}"
    engine = create_engine("mssql+pyodbc://", creator=lambda: pyodbc.connect(connection_string), echo=True)
    HousingFinanceBase.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session


if __name__ == "__main__":
    """
    Example usage
    """
    HfsSession = session_for_hfs(Stage.HOUSING_STAGING)
    with HfsSession.begin() as session:
        res = session.query(GoogleFileSetting).where(GoogleFileSetting.Label == "ChargesOLD").all()
        print(res[0].GoogleIdentifier)
