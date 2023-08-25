import pyodbc
from sqlalchemy import create_engine
from psycopg2 import connect as psycopg2_connect
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession

from aws.src.database.rds.cautionary_alerts.entities.PropertyAlertNew import Base as CautionaryAlertsBase
from aws.src.database.rds.housing_finance.entities.GoogleFileSetting import Base as HousingFinanceBase


def connect_to_local_db(
        db_name: str, username: str, password: str,
        db_type="postgresql", expire_on_commit=True, local_port=5432
        ) -> sessionmaker[SQLAlchemySession]:
    """
    Connect to local postgres database
    :param db_name: Name of database to connect to
    :param db_type: Type of database to connect to (postgresql or mssql)
    :param expire_on_commit: Select to persist detached objects after transaction commit
    :param local_port: Port to connect to locally
    :return: SQLAlchemy sessionmaker - to be used as a context manager
    """
    if db_type == "postgresql":
        connection_string = f"postgresql://{username}:{password}@localhost:{local_port}/{db_name}"
        engine = create_engine("postgresql+psycopg2://", creator=lambda: psycopg2_connect(connection_string), echo=True)
        CautionaryAlertsBase.metadata.create_all(bind=engine)
    elif db_type == "mssql":
        # driver = "/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.4.1"
        try:
            driver = [driver for driver in pyodbc.drivers() if "SQL Server" in driver][0]
        except IndexError:
            raise IndexError("No ODBC Driver 17 for SQL Server found. Please install the driver and try again.")
        connection_string = f"DRIVER={driver};SERVER=127.0.0.1,1433;DATABASE=sow2b;UID={username};PWD={password}"
        engine = create_engine("mssql+pyodbc://", creator=lambda: pyodbc.connect(connection_string), echo=True)
        HousingFinanceBase.metadata.create_all(bind=engine)
    else:
        raise ValueError(f"db_type must be either 'postgresql' or 'mssql', not {db_type}")

    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session
