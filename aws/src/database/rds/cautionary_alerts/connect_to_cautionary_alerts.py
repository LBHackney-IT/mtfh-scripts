"""
Connect to RDS instance via port forwarding and execute SQL queries
"""
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession
from sqlalchemy.orm.exc import DetachedInstanceError

from aws.src.database.entities.PropertyAlertNewEntity import PropertyAlertNewEntity as CautionaryAlert, Base

from mypy_boto3_ssm import SSMClient

from aws.src.authentication.generate_aws_resource import generate_aws_service
from aws.src.database.rds.utils.connect_to_local_db import connect_to_local_db
from enums.enums import Stage

from random import randint


def connect_to_cautionary_alerts_db(stage: Stage, expire_on_commit=True, local_port=5432) -> sessionmaker[SQLAlchemySession]:
    """
    Connect to cautionary alerts database
    :param stage: Stage to connect to
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """
    assert isinstance(stage, Stage), f"stage must be of type Stage, not {type(stage)}"

    path_stage = stage.to_path_variable()
    pg_username_path = f"/uh-api/{path_stage}/postgres-username"
    pg_password_path = f"/uh-api/{path_stage}/postgres-password"

    ssm: SSMClient = generate_aws_service('ssm', stage, 'client')
    username = ssm.get_parameter(Name=pg_username_path)['Parameter']['Value']
    password = ssm.get_parameter(Name=pg_password_path)['Parameter']['Value']

    Session = connect_to_local_db("uh_mirror", username, password, "postgresql", expire_on_commit, local_port)

    return Session


if __name__ == "__main__":
    """
    Example usage
    """
    session_stage = Stage.BASE_STAGING

    # Expire on commit means that objects can only be accessed within the "with" clause and remain connected to the db
    CaSession = connect_to_cautionary_alerts_db(session_stage, expire_on_commit=True)
    with CaSession.begin() as session:
        alerts = session.query(CautionaryAlert) \
            .where(CautionaryAlert.alert_id.is_not(None)) \
            .where(CautionaryAlert.person_name.contains("A")) \
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
    CaSession = connect_to_cautionary_alerts_db(session_stage, expire_on_commit=False)
    with CaSession.begin() as session:
        alerts = session.query(CautionaryAlert) \
            .filter(CautionaryAlert.alert_id == "5d31e525-99af-4264-9e4f-891b1828043b") \
            .all()

        alert = alerts[0]
        # Update value, will sync with db when exiting the "with" clause
        alert.address = f"{randint(1, 1000)}  TEST_NEW_ADDRESS"

    # Can still be accessed outside of the "with" clause
    print(alerts[0].address)
