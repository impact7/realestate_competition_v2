import pandas as pd
from typing import Dict
from app.modules.utils.db_client import DBClient

class UploadParquetToDB:
    def __init__(self):
        pass

    @classmethod
    def create(cls) -> 'UploadParquetToDB':
        return UploadParquetToDB()

    def upload_parquet_to_db(self, dict_db: Dict, str_schema_name: str,
                             str_table_name: str, df: pd.DataFrame):
        client = DBClient.create(**dict_db)

        client.make_for_parquet(str_schema_name, str_table_name, df)


