"""
Connect to RDS instance via port forwarding and execute SQL queries
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SA_Session
from psycopg2 import connect as psycopg2_connect
from aws.database.rds.repairs.entities.AddressStore \
    import AddressStore, Base as RepairsBase

from mypy_boto3_ssm import SSMClient

from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage


def session_for_repairs(stage: Stage, expire_on_commit=True, local_port=5432) -> sessionmaker[SA_Session]:
    """
    Connect to repairs database
    :param stage: Stage to connect to
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """
    assert isinstance(stage, Stage), f"stage must be of type Stage, not {type(stage)}"

    pg_username_path = f"/repairs-api/{stage.to_env_name()}/postgres-username"
    pg_password_path = f"/repairs-api/{stage.to_env_name()}/postgres-password"

    ssm: SSMClient = generate_aws_service('ssm', stage)
    username = ssm.get_parameter(Name=pg_username_path)['Parameter']['Value']
    password = ssm.get_parameter(Name=pg_password_path)['Parameter']['Value']

    connection_string = f"postgresql://{username}:{password}@localhost:{local_port}/repairs_db"
    engine = create_engine("postgresql+psycopg2://", creator=lambda: psycopg2_connect(connection_string), echo=True)
    RepairsBase.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session


if __name__ == "__main__":
    """
    Example usage
    """
    session_stage = Stage.HOUSING_STAGING
    RepairsSession = session_for_repairs(session_stage, expire_on_commit=True)
    with RepairsSession.begin() as session:
        addresses = session.query(AddressStore) \
            .where(AddressStore.address.is_not(None)) \
            .where(AddressStore.postcode.contains("E8")) \
            .limit(10) \
            .all()

        print(addresses[0].address)
