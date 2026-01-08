import pandas as pd
from typing import List
from app.modules.utils.fileutil import FileUtil

class ExtractDataDefinition:
    def __init__(self):
        pass

    @classmethod
    def create(cls) -> "ExtractDataDefinition":
        return ExtractDataDefinition()

    def extract_data(self, str_excel_file_path: str,
                     str_sheet_name: str,
                     str_parquet_file_path: str,
                     lst_column_names: List = None):

        df = FileUtil.get_instance().read_excel(str_excel_file_path, str_sheet_name)

        if lst_column_names is not None:
            df.columns = lst_column_names

        FileUtil.get_instance().save_parquet(df, str_parquet_file_path)
