import numpy as np


# ------------------------------------------------------------------------
# functions
# ------------------------------------------------------------------------
def fmt_columns(columns):
    """Transform a list of columns to a string that is valid as MySQL input.

    Parameters
    ----------
    columns: list
        List of columns to be formated as a string

    Returns
    -------
    columns_string: str
        Formated string with the list of columns.
    """
    columns_string = str([esc_chars(col) for col in columns])

    # if columns is an numpy array.  Insert the missing commas.
    if(type(columns) == np.ndarray):
        columns_string = columns_string.replace(' ', ', ')

    # remove every parentheses and quotation mark from the string.
    for char in ['[', ']', '(', ')', '\'']:
        columns_string = columns_string.replace(char, '')

    return columns_string


def fmt_where(**kwargs):
    """Creates a valid WHERE string statement for MySQL.
    """
    columns = list(kwargs)
    
    cond_expr = [None for _ in range(len(columns))]
    for i, col in enumerate(columns):
        val = esc_chars(kwargs[col])
        col = esc_chars(col)

        if(type(val) == str):
            cond_expr[i] = f'{col}=\'{val}\''
        else:
            cond_expr[i] = f'{col}={val}'

    where_string = ' AND '.join(cond_expr)
    return where_string


def esc_chars(s):
    if(type(s) == str):
        s = s.replace('\'', '\\\'')
        s = s.replace('\"', '\\\"')
    
    return s