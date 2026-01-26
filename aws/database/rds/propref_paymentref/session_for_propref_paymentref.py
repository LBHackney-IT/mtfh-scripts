"""
Connect to RDS instance via port forwarding and execute SQL queries
"""

# pylint: disable=E1136

from mypy_boto3_ssm import SSMClient
from psycopg2 import connect as psycopg2_connect
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SA_Session
from sqlalchemy.orm import sessionmaker

from aws.authentication.generate_aws_resource import generate_aws_service
from aws.database.rds.propref_paymentref.entities import propref_paymentref as entities
from enums.enums import Stage


def session_for_propref_paymentref(
    stage: Stage, expire_on_commit=True, local_port=5432
) -> sessionmaker[SA_Session]:
    """
    Connect to propref_paymentref database
    :param stage: Stage to connect to
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """
    pg_username_path = f"/housing-finance/{stage.to_env_name()}/postgres-username"
    pg_password_path = f"/housing-finance/{stage.to_env_name()}/postgres-password"

    ssm: SSMClient = generate_aws_service("ssm", stage)
    username = ssm.get_parameter(Name=pg_username_path)["Parameter"].get("Value")
    password = ssm.get_parameter(Name=pg_password_path)["Parameter"].get("Value")
    assert username and password, "Username and password must not be None"

    connection_string = f"postgresql://{username}:{password}@localhost:{local_port}/financedb{stage.to_env_name().lower()}"
    engine = create_engine(
        "postgresql+psycopg2://",
        creator=lambda: psycopg2_connect(connection_string),
        # echo=True,
    )
    entities.Base.metadata.reflect(bind=engine)

    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session


def main():
    """Example usage of session_for_propref_paymentref"""
    Session = session_for_propref_paymentref(Stage.HOUSING_DEVELOPMENT)
    with Session() as session:
        results = session.query(entities.ProprefPaymentref)
        count = results.count()
        print(f"Total records in propref_paymentref: {count}")
        first_five = results.limit(5).all()
        for row in first_five:
            print(row)


if __name__ == "__main__":
    main()
