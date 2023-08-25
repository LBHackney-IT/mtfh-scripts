"""
Note: Ensure to install unixodbc to be able to use pyodbc
"""

from mypy_boto3_ssm import SSMClient
from sqlalchemy.orm import sessionmaker, Session as SA_Session

from aws.src.authentication.generate_aws_resource import generate_aws_service
from aws.src.database.rds.housing_finance.entities.GoogleFileSetting import GoogleFileSetting
from aws.src.database.rds.utils.connect_to_local_db import connect_to_local_db
from enums.enums import Stage


def connect_to_hfs_db(stage: Stage, expire_on_commit=True, local_port=1433) -> sessionmaker[SA_Session]:
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

    Session = connect_to_local_db("sow2b", username, password, "mssql", expire_on_commit, local_port)

    return Session


if __name__ == "__main__":
    """
    Example usage
    """
    HfsSession = connect_to_hfs_db(Stage.HOUSING_STAGING)
    with HfsSession.begin() as session:
        res = session.query(GoogleFileSetting).where(GoogleFileSetting.Label == "ChargesOLD").all()
        print(res[0].GoogleIdentifier)
