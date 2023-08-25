from sqlalchemy import create_engine
from psycopg2 import connect
from sqlalchemy.orm import sessionmaker, Session as SQLAlchemySession

from aws.src.database.entities.PropertyAlertNewEntity import Base


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
    elif db_type == "mssql":
        connection_string = f"mssql+pyodbc://{username}:{password}@localhost:{local_port}/{db_name}"
    else:
        raise ValueError(f"db_type must be either 'postgresql' or 'mssql', not {db_type}")

    engine = create_engine(f"{db_type}+psycopg2://", creator=lambda: connect(connection_string), echo=True)
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)

    return Session
