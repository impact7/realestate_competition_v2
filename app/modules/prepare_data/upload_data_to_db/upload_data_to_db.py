import os
import json
import logging
from typing import Dict
from app.modules.prepare_data.datatype_transformer.schema_transformer import CreateTableTransformer
from app.modules.utils.db_client import DBClient
from app.modules.utils.fileutil import FileUtil
from app.modules.settings.config import AppConfig

logger = logging.getLogger(__name__)
dict_config = AppConfig.get_instance().get_config()

class UploadDataToDB:
    def __init__(self):
        pass

    @classmethod
    def create(cls) -> 'UploadDataToDB':
        return UploadDataToDB()

    def upload_data_to_db(self, dict_db: Dict, str_dataname: str, str_define_data_folder: str,
                          str_make_data_for_db_folder: str, str_schema_name: str, refresh_flag : bool = True) -> None:
        """
        テキストデータをDBにアップロードする

        :param str_json_path: データ定義のJSONのPath
        :param refresh_flag: テーブルを削除して作り直すか？
        """
        db_client = DBClient.create(**dict_db)

        str_table_name = '{}.{}'.format(
            str_schema_name, str_dataname
        )

        if refresh_flag:
            logger.info('DROP TABLE {}'.format(str_table_name))
            db_client.drop_table(str_table_name)

        logger.info('CREATE TABLE {}'.format(str_table_name))

        str_create_table_sql = self._create_table_sql_from_definition(
            dict_db['dbms_name'], str_dataname, str_define_data_folder, str_schema_name
        )

        db_client.execute_sql(str_create_table_sql)

        # フォルダに含まれる全てのファイルをstr_table_nameによりテーブル化
        str_glob_path = os.path.join(str_make_data_for_db_folder, str_dataname, '*')

        lst_files = FileUtil.get_instance().glob(str_glob_path)

        for str_file_path in lst_files:
            db_client.copy_from_data(str_table_name, str_file_path)

    def _create_table_sql_from_definition(self, str_dbms_name: str,
                                          str_dataname: str, str_define_data_folder: str,
                                          str_schema_name: str) -> str:
        first_flag = True

        str_define_data_path = os.path.join(
            str_define_data_folder,
            '{}.json'.format(str_dataname)
        )

        # Polarsの型定義をCreate Table(DB)の型定義に変換する、変換後がdict_ct_define
        dict_define_data = FileUtil.get_instance().load_json(str_define_data_path)

        transformer = CreateTableTransformer.create(str_dbms_name)

        dict_ct_define = transformer.transform(dict_define_data)

        str_ct_define = ''

        for key, value in dict_ct_define.items():
            str_revision_name = key.replace('(', '_').replace(')', '')

            if first_flag:
                str_ct_define += '{} {}'.format(str_revision_name, value)
                first_flag = False
            else:
                str_ct_define += ',{} {}'.format(str_revision_name, value)

        str_create_table_sql = 'create table if not exists {}.{} ({})'.format(
                        str_schema_name,
                        str_dataname,
                        str_ct_define
        )

        return str_create_table_sql


