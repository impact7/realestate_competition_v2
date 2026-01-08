import os
import json
import logging
import polars as pl
from typing import Dict
from app.modules.utils.fileutil import FileUtil
from app.modules.prepare_data.datatype_transformer.schema_transformer import PolarsSchemaTransformer

logger = logging.getLogger(__name__)

class MakeDataForDB:
    def __init__(self):
        pass

    @classmethod
    def create(cls) -> "MakeDataForDB":
        return MakeDataForDB()

    def make_db_data(self, str_dataname: str, str_raw_data_folder: str, str_define_data_folder: str,
                     str_make_data_for_db_folder: str, define_infer_flag: bool = True) -> None:

        str_define_json_path = os.path.join(str_define_data_folder, '{}.json'.format(str_dataname))

        # schemaデータを読み込む
        if not define_infer_flag:
            dict_schema = FileUtil.get_instance().load_json(str_define_json_path)
            dict_schema = PolarsSchemaTransformer.transform(dict_schema)
        else:
            dict_schema = None

        lst_files = FileUtil.get_instance().glob(os.path.join(str_raw_data_folder, str_dataname, '*'))

        logger.info(dict_schema)

        for str_file_path in lst_files:
            df = FileUtil.get_instance().load_csv_tsv_by_polars(str_file_path, dict_schema)

            # schemaがない場合は保存する
            if dict_schema is None:
                dict_schema = self._make_and_save_schema_by_data(df, str_define_json_path)

            str_save_folder = os.path.join(str_make_data_for_db_folder, str_dataname)

            # 保存するファイル名
            str_basename = os.path.basename(str_file_path)
            str_save_path = os.path.join(str_save_folder, str_basename)

            logger.info('Save file {}'.format(str_save_path))

            FileUtil.get_instance().save_csv_tsv_by_polars(df, str_save_path)

    def _make_and_save_schema_by_data(self, df: pl.DataFrame, str_define_json_path: str) -> Dict:
        dict_schema = {}

        for column_name, data_type in zip(df.columns, df.dtypes):
            dict_schema[column_name] = str(data_type)

        FileUtil.get_instance().save_json(dict_schema, str_define_json_path)

        return dict_schema
