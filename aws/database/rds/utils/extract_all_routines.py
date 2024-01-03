"""
Extracts all stored procedures and functions from a database and saves them to a directory
"""
import os

from sqlalchemy import text, Row
from sqlalchemy.orm import Session


OUTFILE_PATH = "stored_procedures"


def extract_routine(session: Session, routine_name: str) -> str:
    """
    Extracts a stored procedure or function from the database and returns it as a string
    """
    routine_text_query = text(f'SELECT OBJECT_DEFINITION(OBJECT_ID(N\'{routine_name}\'))')
    database_routine = session.execute(routine_text_query).all()

    routine_text = str(database_routine[0][0]).encode('utf-8').decode('utf-8').strip()

    return routine_text


def extract_all_routines(session: Session, db_name: str):
    """
    Extracts all stored procedures and functions from a database
    and saves them as sql files in a directory
    :param session: SQLAlchemy session
    """
    session.execute(text(f"USE {db_name}"))
    all_routine_names_query = text("SELECT name FROM sys.objects WHERE type IN ('P', 'FN')")
    db_routines: list[Row] = session.execute(all_routine_names_query).all()

    routine_names: list[str] = [routine_name[0] for routine_name in db_routines]

    if not os.path.exists(OUTFILE_PATH):
        os.makedirs(OUTFILE_PATH)

    # Fetch each routine and save it to a .sql file
    for routine_name in routine_names:
        routine_text = extract_routine(session, routine_name)
        with open(f"{OUTFILE_PATH}/{routine_name}.sql", "w") as f:
            f.write(routine_text)
