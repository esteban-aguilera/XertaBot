import pandas as pd
import pathlib
import sqlite3

# package imports    
from .. import manager


# --------------------------------------------------------------------------------
# functions
# --------------------------------------------------------------------------------
def get_commands(conn):
    """Returns list of users.
    
    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    privilege: int or None
        Returns users with specified privilege level.
    
    Returns
    -------
    df: np.ndarray
        Array with the jokes data from the MySQL table.
    """
    df = manager.get_table(conn, 'commands')

    return df


def insert_command(conn, user_id, command):
    """Inserts a user to users table in database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    user_id: int
        Telegram id of the user

    command: str
        Name of the command being used

    Returns
    -------
    nrows: int
        number of rows inserted to the table.
    """
    nrows = manager.insert_row(conn, 'commands',
        ['user_id', 'command'],
        [user_id, command]
    )

    return nrows