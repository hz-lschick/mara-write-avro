"""Commands for writing AVRO files"""

import json
import pathlib
from typing import Callable, Union

import pandas as pd
import pandavro as pdx
from mara_pipelines import config, pipelines
from mara_pipelines.logging import logger
from mara_page import _, html
from .. import read_dataframe

class WriteAvroFile(pipelines.Command):
    """Writes data to a local AVRO file"""

    def __init__(self, file_name: str, schema = None, schema_file_name: str = None,
                 sql_query: Union[str, Callable] = None, sql_file_name: str = None,
                 replace: {str: str} = None, db_alias: str = None) -> None:
        """
        Writes an sql file or sql query result into an AVRO file

        Args:
            file_name: The AVRO file to write to
            schema: The AVRO schema as dict
            schema_file_name: The name of the file which contains the AVRO schema
            sql_query: The SQL query to run as a string
            sql_file_name: The name of the file with the SQL query to run (relative to the directory of the parent pipeline)
            replace: A set of replacements to perform against the sql query `{'replace`: 'with', ..}`
        """
        if (not (sql_query or sql_file_name)) or (sql_query and sql_file_name):
            raise ValueError('Please provide either sql_query or sql_file_name (but not both)')
        if schema and schema_file_name:
            raise ValueError('schema_dict and schema_file_name can not be provided both')

        self.file_name = file_name
        self.schema = schema
        self.schema_file_name = schema_file_name
        self._sql_query = sql_query
        self.sql_file_name = sql_file_name
        self.replace = replace
        self._db_alias = db_alias

    @property
    def db_alias(self):
        return self._db_alias or config.default_db_alias()

    @property
    def sql_query(self):
        return self._sql_query() if callable(self._sql_query) else self._sql_query

    def sql_file_path(self) -> pathlib.Path:
        # Get the first pipeline in the tree (don't reach root)
        pipeline_candidate = self
        while not isinstance(pipeline_candidate, pipelines.Pipeline):
            pipeline_candidate = pipeline_candidate.parent
        return pipeline_candidate.base_path() / self.sql_file_name

    def schema_file_path(self) -> pathlib.Path:
        # Get the first pipeline in the tree (don't reach root)
        pipeline_candidate = self
        while not isinstance(pipeline_candidate, pipelines.Pipeline):
            pipeline_candidate = pipeline_candidate.parent
        return pipeline_candidate.base_path() / self.schema_file_name

    def get_schema(self):
        if self.schema_file_name:
            schema_file_path = str(self.schema_file_path().absolute())
            logger.log(f'Load AVRO schema from file {schema_file_path}', format=logger.Format.ITALICS)
            with open(schema_file_path, 'r') as f:
                return json.load(f)
        return self.schema

    def get_sql_query(self):
        sql_query = None

        if self.sql_file_path:
            sql_query_file_path=str(self.sql_file_path().absolute())
            logger.log(f'Read SQL query from file {sql_query_file_path}', format=logger.Format.ITALICS)
            with open(sql_query_file_path, 'r') as f:
                sql_query = f.read()
        if self.sql_query:
            sql_query = self.sql_query

        if self.replace:
            for key, value in self.replace:
                sql_query = sql_query.replace(key, value)

        return sql_query

    def run(self) -> bool:
        """
        Runs the command

        Returns:
            False on failure
        """

        # load schema
        schema = self.get_schema()

        # load sql file
        sql_query = self.get_sql_query()

        # query data frame from db
        logger.log(f'Read data from SQL', format=logger.Format.ITALICS)
        df = read_dataframe(self.db_alias, sql_query)

        # write avro file
        avro_file_path = f"{pathlib.Path(config.data_dir()) / self.file_name}"
        logger.log(f'Write to AVRO file {avro_file_path}', format=logger.Format.ITALICS)
        pdx.to_avro(avro_file_path, df, schema=schema)

        return True

    def html_doc_items(self) -> [(str, str)]:
        return [('file name', _.i[self.file_name]),
                ## TODO: exted here with: schema, schema_file_name, sql_query, sql_file_name, replace
                ('db alias', _.tt[self.db_alias])]