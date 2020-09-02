import mysql.connector
import numpy as np
import os
import pandas as pd


# local modules
from .formater import fmt_columns, fmt_where


# ------------------------------------------------------------------------
# generic functions
# ------------------------------------------------------------------------
def insert_dataframe(conn, table, df, repeated_entries=True):
    """Insert DataFrame into any table in a MySQL database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the dataframe.

    df: list
        DataFrame with the values to be incorporated in the database.

    Returns
    -------
    nrows: int
        number of rows inserted to the table.
    """
    columns = df.columns.values
    data = np.array(df)

    nrows = 0
    for values in data:
        nrows += insert_row(conn, table, columns, values,
                            repeated_entries=repeated_entries)
    
    return nrows


def insert_row(conn, table, columns, values, repeated_entries=True):
    """Insert a single row of data into a particular table in a database.
    
    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the dataframe.

    columns: list
        Columns that are going to be used to insert data

    values: list
        Values that are going to be inserted in the database

    Returns
    -------
    nrows: int
        number of rows inserted to the table.
    """
    columns_str = fmt_columns(columns)
    values = tuple(values)  # values must be a tuple

    # MySQL command
    sql_command = f'INSERT INTO {table} ({columns_str}) VALUES (' + \
        ', '.join(['%s']*len(columns)) + \
        ')'

    # MySQL interaction
    cursor = conn.cursor()

    try:
        cursor.execute(sql_command, values)
        conn.commit()
    
        nrows = 1

        # # check if data already exists
        # table = get_table(conn, table,
        #     where={columns[i]:values[i]
        #         for i in range(len(values))}
        # )

        # # insert data to table only if it does not exist.
        # if(len(table) == 0):
        #     cursor.execute(sql_command, values)
        #     conn.commit()
        
        #     nrows = 1
        # else:
        #     nrows = 0
    except mysql.connector.errors.IntegrityError:
        nrows = 0

    cursor.close()
    return nrows


def update_column(conn, table, column, value, **kwargs):
    """Update column of table in a database.
    
    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the database.

    column: list
        Column that is going to be updated

    value: list
        Target value that is going to be used to update table.

    **kwargs:
        arguments that are going to be used to determine WHERE the
        data will be updated.

    Returns
    -------
    nrows: int
        number of rows inserted to the table.
    """
    # MySQL interaction
    cursor = conn.cursor()

    # execute MySQL command
    if(len(kwargs) > 0):
        where_str = fmt_where(**kwargs)
        sql_command = f'UPDATE {table} SET {column}=%s WHERE {where_str}'
    else:
        sql_command = f'UPDATE {table} SET {column}=%s'

    cursor.execute(sql_command, (value,))
    conn.commit()
    
    nrows = cursor.rowcount
    
    # close interaction
    cursor.close()

    return nrows


def delete_values(conn, table, **kwargs):
    """Delete values of a table.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the dataframe.

    **kwargs:
        arguments that are going to be used to determine WHERE the
        data will be deleted.
    """    
    cursor = conn.cursor()
    if(len(kwargs) > 0):
        where_str = fmt_where(**kwargs)
        cursor.execute(f'DELETE FROM {table} WHERE {where_str}')
    else:
        cursor.execute(f'DELETE FROM {table}')
    conn.commit()


def export_table(conn, table, **kwargs):
    """Exports a table from the database to a csv file.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the dataframe.

    filename: str, optional
        filename of the csv table.  By default it is the table name.
    
    columns: list, optional
        List with columns names.  By default it takes every column in the table.

    where: str, optional
        Conditional expression that must be satisfied.
    """
    columns = kwargs.get('columns', get_columns(conn, table))
    where = kwargs.get('where', None)
    filename = kwargs.get('filename', table)

    df = get_table(conn, table, columns=columns, where=where)
    
    if(filename[-4:] == ".csv"):
        df.to_csv(filename)
    else:
        df.to_csv(f'{filename}.csv')


def get_table(conn, table, **kwargs):
    """Get table from database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the dataframe.
    
    columns: list, optional
        List with columns names.  By default it takes every column in the table.

    where: str, optional
        Conditional expression that must be satisfied.

    Returns
    -------
    df: np.ndarray
        Array with the data from the MySQL table.
    """
    columns = kwargs.get('columns', get_columns(conn, table))
    where = kwargs.get('where', None)

    # format columns as a string
    columns_str = fmt_columns(columns)

    # MySQL interaction
    cursor = conn.cursor()

    if(where is None):
         # obtain every column from the table
        sql_command = f'SELECT {columns_str} FROM {table}'
    elif(type(where) == dict):
        where_str = fmt_where(**where)
        
        # obtain the colums of the rows where the condition is satisfied
        sql_command = f'SELECT {columns_str} FROM {table} WHERE {where_str}'
    else:
        raise TypeError('\'where\' must be a dict or None.')
    
    cursor.execute(sql_command)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    return df


def get_columns(conn, table):
    """Get every column from a table in database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the dataframe.

    Returns
    -------
    columns: list
        List with columns names.
    """    
    cursor = conn.cursor()
    cursor.execute(f"DESC {table}")

    # obtain tables as an array of tuples.
    tables = cursor.fetchall()
    
    # extract the table name from the tuple.
    columns = [row[0] for row in tables]

    return columns


def reset_table(conn, table):
    """Resets every entry from a particular table.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    table: str
        Table name inside the dataframe.
    """    
    cursor = conn.cursor()
    cursor.execute(f'DELETE FROM {table}')
    cursor.execute(f'ALTER TABLE {table} AUTO_INCREMENT = 1')


def get_tables(conn):
    """Get every table in the database.

    Parameters
    ----------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.

    Returns
    -------
    tables: list
        List with table names.
    """
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")

    # obtain tables as an array of tuples.  Each tuple has length == 1
    tables = cursor.fetchall()
    
    # extract the table name from the tuple.
    tables = [table[0] for table in tables]

    return tables


def connect():
    """Stablishes a connection with the MySQL server.  To assign every
    parameter, the following scheme should be followed:

    import os
    
    os.environ['MYSQL_HOST'] = 'localhost'
    os.environ['MYSQL_USER'] = 'username'
    os.environ['MYSQL_PASSWORD'] = 'pass1234'
    os.environ['MYSQL_DATABASE'] = 'dabase_name'

    Parameters
    ----------
    MYSQL_HOST: str
        MySQL host.
    
    MYSQL_USERNAME: str
        MySQL username.

    MYSQL_PASSWORD: str
        MySQL password.

    MYSQL_DATABASE: str
        Database name.

    Returns
    -------
    conn: mysql.connector.connection_cext.CMySQLConnection
        connection with MySQL server.
    """
    host = os.environ.get('MYSQL_HOST')
    user = os.environ.get('MYSQL_USERNAME')
    password = os.environ.get('MYSQL_PASSWORD')
    database = os.environ.get('MYSQL_DATABASE')

    # stablish connection
    conn = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )

    return conn