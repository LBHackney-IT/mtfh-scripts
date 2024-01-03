"""
Connect to RDS instance via port forwarding and execute SQL queries
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SA_Session
from sqlalchemy.orm.exc import DetachedInstanceError
from psycopg2 import connect as psycopg2_connect
from aws.database.rds.uh_mirror.entities.PropertyAlertNew \
    import PropertyAlertNew, Base as UHMirrorBase

from mypy_boto3_ssm import SSMClient

from aws.authentication.generate_aws_resource import generate_aws_service
from enums.enums import Stage

from random import randint


def session_for_uh_mirror(stage: Stage, expire_on_commit=True, local_port=5432) -> sessionmaker[SA_Session]:
    """
    Connect to cautionary alerts database
    :param stage: Stage to connect to
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """
    assert isinstance(stage, Stage), f"stage must be of type Stage, not {type(stage)}"

    pg_username_path = f"/uh-api/{stage.to_env_name()}/postgres-username"
    pg_password_path = f"/uh-api/{stage.to_env_name()}/postgres-password"

    ssm: SSMClient = generate_aws_service('ssm', stage, 'client')
    username = ssm.get_parameter(Name=pg_username_path)['Parameter']['Value']
    password = ssm.get_parameter(Name=pg_password_path)['Parameter']['Value']

    connection_string = f"postgresql://{username}:{password}@localhost:{local_port}/uh_mirror"
    engine = create_engine("postgresql+psycopg2://", creator=lambda: psycopg2_connect(connection_string), echo=True)
    UHMirrorBase.metadata.create_all(bind=engine)

    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session


if __name__ == "__main__":
    """
    Example usage
    """
    session_stage = Stage.BASE_STAGING

    # Expire on commit means that objects can only be accessed within the "with" clause and remain connected to the db
    CaSession = session_for_uh_mirror(session_stage, expire_on_commit=True)
    with CaSession.begin() as session:
        alerts = session.query(PropertyAlertNew) \
            .where(PropertyAlertNew.alert_id.is_not(None)) \
            .where(PropertyAlertNew.person_name.contains("A")) \
            .limit(10) \
            .all()

        # Use the entity object directly
        print(alerts[0].person_name)

        # Convert to a dictionary
        dict_alert = alerts[0].to_dict()["address"]

    try:
        # This will throw an error as the object is detached from the session
        print(alerts[0].person_name)
    except DetachedInstanceError as e:
        print("Cannot access object outside of 'with' clause")

    # With expire_on_commit set to False, objects can be accessed outside of the "with" clause, but will not sync to db
    CaSession = session_for_uh_mirror(session_stage, expire_on_commit=False)
    with CaSession.begin() as session:
        alerts = session.query(PropertyAlertNew) \
            .filter(PropertyAlertNew.alert_id == "5d31e525-99af-4264-9e4f-891b1828043b") \
            .all()

        alert = alerts[0]
        # Update value, will sync with db when exiting the "with" clause
        alert.address = f"{randint(1, 1000)}  TEST_NEW_ADDRESS"

    # Can still be accessed outside of the "with" clause
    print(alerts[0].address)
