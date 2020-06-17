from functools import singledispatch

import pandas as pd
from mara_db import dbs

@singledispatch
def read_dataframe(db: object, sql_query: str):
    """
    Runs a SQL query against a database to receive a pandas DataFrame

    Args:
        db: The database in which to run the query (either an alias or a `dbs.DB` object

    Returns:
        A pandas DataFrame object
    """
    raise NotImplementedError(f'Please implement query_command for type "{db.__class__.__name__}"')


@read_dataframe.register(str)
def __(alias: str, sql_query: str):
    return read_dataframe(dbs.db(alias), sql_query=sql_query)


@read_dataframe.register(dbs.SQLServerDB)
def __(db: dbs.SQLServerDB, sql_query: str):
    import pyodbc # requires https://github.com/mkleehammer/pyodbc/wiki/Install

    connection = pyodbc.connect(f"DRIVER={{{db.odbc_driver}}};SERVER={db.host};DATABASE={db.database};UID={db.user};PWD={db.password}")

    return pd.read_sql_query(sql_query, connection)
