import os
import json
import yaml
import pickle
import logging
import pandas as pd
import polars as pl
import geopandas as gpd
from typing import Dict
from app.modules.settings.settings import DEFAULT_PROTOCOL, DICT_S3_PARAMS
from app.modules.utils.filesystem import FileSystem

logger = logging.getLogger(__name__)

class FileUtil:
    instance = None

    # __new()__をオーバーライド
    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(cls)

        return cls.instance

    def __init__(self):
        self.dict_s3_params = None

    @classmethod
    def get_instance(cls) -> 'FileUtil':
        if cls.instance is None:
            cls.instance = cls()

        return cls.instance


    @classmethod
    def reset_instance(cls):
        cls.instance = None

    def set_s3_params(self, dict_s3_params: dict):
        self.dict_s3_params = dict_s3_params

    def clear_s3_params(self):
        self.dict_s3_params = None

    def save_pickle(self, obj: object, str_save_path: str):
        fs = FileSystem.create(str_save_path, self.dict_s3_params)

        self._makedirs(fs, str_save_path)

        with fs.open(str_save_path, 'wb') as f:
            pickle.dump(obj, f, protocol=DEFAULT_PROTOCOL)

    def load_pickle(self, str_load_path: str) -> object:
        fs = FileSystem.create(str_load_path, self.dict_s3_params)

        with fs.open(str_load_path, 'rb') as f:
            return pickle.load(f)

    def _makedirs(self, fs: FileSystem, str_save_path: str):
        str_save_folder = os.path.dirname(str_save_path)

        if not fs.exists(str_save_folder):
            fs.makedirs(str_save_folder)

    def save_json(self, dict_json: Dict, str_save_path: str):
        fs = FileSystem.create(str_save_path, self.dict_s3_params)

        self._makedirs(fs, str_save_path)

        with fs.open(str_save_path, 'w') as f:
            json.dump(dict_json, f)

    def load_json(self, str_load_path: str) -> Dict:
        fs = FileSystem.create(str_load_path, self.dict_s3_params)

        with fs.open(str_load_path, 'r') as f:
            return json.load(f)

    def save_parquet(self, df: pd.DataFrame, str_save_path: str):
        fs = FileSystem.create(str_save_path, self.dict_s3_params)

        self._makedirs(fs, str_save_path)

        with fs.open(str_save_path, 'wb') as f:
            df.to_parquet(f)

    def save_df_csv(self, df: pd.DataFrame, str_save_path: str):
        fs = FileSystem.create(str_save_path, self.dict_s3_params)

        self._makedirs(fs, str_save_path)

        with fs.open(str_save_path, 'w') as f:
            df.to_csv(f, header=None, index=False)

    def read_excel(self, str_excel_file_path: str, str_sheet_name: str) -> pd.DataFrame:
        fs = FileSystem.create(str_excel_file_path, self.dict_s3_params)

        with fs.open(str_excel_file_path, 'rb') as f:
            df = pd.read_excel(f, str_sheet_name)

        return df

    def read_geojson(self, str_geojson_file_path: str):
        fs = FileSystem.create(str_geojson_file_path, self.dict_s3_params)

        with fs.open(str_geojson_file_path, 'rb') as f:
            gdf = gpd.read_file(f)

        return gdf


    def load_parquet(self, str_load_path: str) -> pd.DataFrame:
        fs = FileSystem.create(str_load_path, self.dict_s3_params)

        with fs.open(str_load_path, 'rb') as f:
            df = pd.read_parquet(f)

        return df

    def load_yaml(self, str_yaml_path: str, dict_s3_params: Dict = None) -> Dict:
        fs = FileSystem.create(str_yaml_path, dict_s3_params)

        with fs.open(str_yaml_path, 'r') as f:
            dict_config = yaml.load(f, Loader=yaml.FullLoader)

        return dict_config

    def glob(self, str_glob_path: str):
        fs = FileSystem.create(str_glob_path, self.dict_s3_params)

        return fs.glob(str_glob_path)

    def load_csv_tsv_by_polars(self, str_csv_tsv_path: str, dict_schema: Dict = None) -> pl.DataFrame:
        logger.info('load_csv_tsv_by_polars: {}'.format(str_csv_tsv_path))

        fs = FileSystem.create(str_csv_tsv_path, self.dict_s3_params)
        str_separator = self._get_separator(str_csv_tsv_path)

        with fs.open(str_csv_tsv_path, 'r') as f:
            if dict_schema is None:
                df = pl.read_csv(f, separator=str_separator, infer_schema_length=100000)
            else:
                df = pl.read_csv(f, separator=str_separator, schema=dict_schema, truncate_ragged_lines=True)

        return df

    def save_csv_tsv_by_polars(self, df: pl.DataFrame, str_csv_tsv_path: str):
        fs = FileSystem.create(str_csv_tsv_path, self.dict_s3_params)
        str_separator = self._get_separator(str_csv_tsv_path)

        self._makedirs(fs, str_csv_tsv_path)

        with fs.open(str_csv_tsv_path, 'w') as f:
            df.write_csv(f, separator=str_separator)

    def load_image(self, str_image_path: str) -> bytes:
        fs = FileSystem.create(str_image_path, self.dict_s3_params)

        with fs.open(str_image_path, 'rb') as f:
            return f.read()

    def save_image(self, image: bytes, str_image_path: str):
        fs = FileSystem.create(str_image_path, self.dict_s3_params)

        self._makedirs(fs, str_image_path)

        with fs.open(str_image_path, 'wb') as f:
            f.write(image)


    def _get_separator(self, str_file_path: str) -> str:
        if str_file_path.endswith('.csv'):
            return ','
        else:
            return '\t'

    def load_text_file(self, str_text_file_path: str) -> str:
        fs = FileSystem.create(str_text_file_path, self.dict_s3_params)

        with fs.open(str_text_file_path, 'r') as f:
            return f.read()

    def save_text_file(self, str_text: str, str_text_file_path: str):
        fs = FileSystem.create(str_text_file_path, self.dict_s3_params)

        self._makedirs(fs, str_text_file_path)

        with fs.open(str_text_file_path, 'w') as f:
            f.write(str_text)

