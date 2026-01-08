import polars as pl
from typing import Dict
from abc import abstractmethod

class PolarsSchemaTransformer:
    def __init__(self):
        pass

    @classmethod
    def transform(cls, dict_schema: Dict) -> Dict:
        dict_polars_transformer = {
            'String': pl.String(),
            'Int64': pl.Int64(),
            'Float64': pl.Float64(),
            'Date': pl.Date(),
            'Boolean': pl.Boolean(),
        }

        dict_output_schema = {
            key: dict_polars_transformer[item] for key, item in dict_schema.items()
        }

        return dict_output_schema

class CreateTableTransformer:
    def __init__(self):
        pass

    @classmethod
    def create(cls, str_dbms_name: str) -> 'CreateTableTransformer':
        if str_dbms_name == 'PostgreSQL':
            return PostgreSQLCreateTableTransformer()
        elif str_dbms_name == 'BigQuery':
            return BigQueryCreateTableTransformer()

        raise NotImplementedError()

    @abstractmethod
    def transform(cls, dict_define: Dict) -> Dict:
        raise NotImplementedError()

class PostgreSQLCreateTableTransformer(CreateTableTransformer):
    def transform(cls, dict_define: Dict) -> Dict:
        dict_create_table_transformer = {
            'String': 'text',
            'Int64': 'bigint',
            'Float64': 'double precision',
            'Date': 'date',
            'Boolean': 'boolean',
        }

        dict_output_create_table = {
            key: dict_create_table_transformer[item] for key, item in dict_define.items()
        }

        return dict_output_create_table

class BigQueryCreateTableTransformer(CreateTableTransformer):
    def transform(cls, dict_define: Dict) -> Dict:
        dict_create_table_transformer = {
            'String': 'STRING',
            'Int64': 'INT64',
            'Float64': 'FLOAT64',
            'Date': 'DATE',
            'Boolean': 'BOOLEAN',
        }

        dict_output_create_table = {
            key: dict_create_table_transformer[item] for key, item in dict_define.items()
        }

        return dict_output_create_table


