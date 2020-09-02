import pandas as pd
import pathlib
import sqlite3

# package imports    
from .. import manager


# --------------------------------------------------------------------------------
# functions
# --------------------------------------------------------------------------------
def get_users(conn, privilege=None):
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
    if(privilege is None):
        df = manager.get_table(conn, 'users')
    else:
        df = manager.get_table(conn, 'users', where={'privilege':privilege})

    return df
    

def update_user(conn, columns, values):
    """Inserts a user to users table in database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    columns: list
        Columns that are going to be used to insert data

    values: list
        Values that are going to be inserted in the database

    Returns
    -------
    nrows: int
        number of rows inserted to the table.
    """
    nrows = manager.insert_row(conn, 'users', columns, values)
    return nrows


def insert_user(conn, columns, values):
    """Inserts a user to users table in database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    columns: list
        Columns that are going to be used to insert data

    values: list
        Values that are going to be inserted in the database

    Returns
    -------
    nrows: int
        number of rows inserted to the table.
    """
    nrows = manager.insert_row(conn, 'users', columns, values)

    if(nrows == 0):  # update user
        where = {col:val for col, val in zip(columns, values) if col == 'id'}
        
        for column, value in zip(columns, values):
            if(column == 'id'):
                continue
            else:
                manager.update_column(conn, 'users', column, value, **where)
    
    return nrows