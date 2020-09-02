import pathlib
import numpy as np

# package imports
from .. import manager


# --------------------------------------------------------------------------------
# functions
# --------------------------------------------------------------------------------
def random_joke(conn):
    """Extract a random joke from the database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    Returns
    -------
    joke: str
        Random joke from jokes table in database.
    """
    df = get_jokes(conn)    
    idx = np.random.randint( len(df) )
    
    return df.loc[idx, 'joke']


def get_jokes(conn):
    """Get every element from jokes table.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    Returns
    -------
    df: np.ndarray
        Array with the jokes data from the MySQL table.
    """
    df = manager.get_table(conn, 'jokes')
    df.set_index('id')

    return df


def insert_joke(conn, joke):
    """Get every element from jokes table.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    Returns
    -------
    df: np.ndarray
        Array with the jokes data from the MySQL table.
    """
    nrows = manager.insert_row(conn, 'jokes', ['joke'], [joke])

    return nrows