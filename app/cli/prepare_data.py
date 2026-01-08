import logging
import subprocess
from app.modules.settings.config import AppConfig
from app.modules.utils.logger import setup_logging
from app.modules.utils.fileutil import FileUtil
from app.modules.prepare_process.extract_data_definition import ExtractDataDefinition
from app.modules.prepare_process.prepare_station_data import PrepareStationData
from app.modules.prepare_process.make_mesh_data import MakeMeshData
from app.modules.prepare_process.prepare_price_data import PreparePriceData
from app.modules.prepare_data.make_data_for_db.make_data_for_db import MakeDataForDB
from app.modules.prepare_data.upload_data_to_db.upload_data_to_db import UploadDataToDB
from app.modules.prepare_data.upload_parquet_to_db.upload_parquet_to_db import UploadParquetToDB
from app.modules.utils.db_client import DBClient

dict_config = AppConfig.get_instance().get_config()

dict_db = {
    "dbms_name": "PostgreSQL",
    "conn": {
        "host": "host.docker.internal",
        "dbname": "postgres",
        "user": "postgres",
        "password": "passw0rd",
        "port": 5434
    }
}

if __name__ == '__main__':
    setup_logging()

    logger = logging.getLogger(__name__)
    logger.info('Pipeline start.')
    logger.info('run prepare_data_definition_task')

    db_client = DBClient.create(**dict_db)
    db_client.execute_sql('create schema if not exists raw_data')

    str_excel_file_path = '/app/competition_data/data_definition.xlsx'
    str_sheet_name = '③タグマスタ情報'
    str_parquet_file_path = '/app/competition_data/process/tag_master.parquet'
    lst_column_names = ["no", "tag_id", "tag_content", "tag_supplement"]

    extract_data_definition = ExtractDataDefinition.create()

    extract_data_definition.extract_data(
        str_excel_file_path,
        str_sheet_name,
        str_parquet_file_path,
        lst_column_names
    )

    str_excel_file_path = '/app/competition_data/data_definition.xlsx'
    str_sheet_name = '⑤設備情報シート'
    str_parquet_file_path = '/app/competition_data/process/facility_master.parquet'
    lst_column_names = ["no", "tag_id", "tag_content"]

    extract_data_definition = ExtractDataDefinition.create()

    extract_data_definition.extract_data(
        str_excel_file_path,
        str_sheet_name,
        str_parquet_file_path,
        lst_column_names
    )

    str_parquet_file_path = '/app/competition_data/process/station_master.parquet'

    prepare_station_data = PrepareStationData.create()
    prepare_station_data.prepare_data(
        str_parquet_file_path
    )

    make_mesh_data = MakeMeshData()
    make_mesh_data.prepare_data('/app/competition_data/process/mesh_data.parquet')

    prepare_price_data = PreparePriceData()
    prepare_price_data.prepare_data(
        '/app/competition_data/process/price_country_data.parquet',
        '/app/competition_data/process/price_prefecture_data.parquet'
    )

    # train
    make_data_for_db = MakeDataForDB()

    str_raw_data_folder = '/app/competition_data'
    str_define_data_folder = '/app/json/define_data'
    str_make_data_for_db_folder = '/app/make_data_for_db'
    str_schema_name = 'raw_data'

    make_data_for_db.make_db_data('train',
                                  str_raw_data_folder,
                                  str_define_data_folder,
                                  str_make_data_for_db_folder,
                                  True)

    upload_data_to_db = UploadDataToDB.create()

    upload_data_to_db.upload_data_to_db(
        dict_db=dict_db,
        str_dataname='train',
        str_define_data_folder=str_define_data_folder,
        str_make_data_for_db_folder=str_make_data_for_db_folder,
        str_schema_name=str_schema_name,
    )

    # test
    make_data_for_db.make_db_data('test',
                                  str_raw_data_folder,
                                  str_define_data_folder,
                                  str_make_data_for_db_folder,
                                  True)

    upload_data_to_db.upload_data_to_db(
        dict_db=dict_db,
        str_dataname='test',
        str_define_data_folder=str_define_data_folder,
        str_make_data_for_db_folder=str_make_data_for_db_folder,
        str_schema_name=str_schema_name,
    )

    # tag_master
    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'tag_master'

    df = FileUtil.get_instance().load_parquet('/app/competition_data/process/tag_master.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, str_schema_name, str_table_name, df)

    # facility_master
    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'facility_master'

    df = FileUtil.get_instance().load_parquet('/app/competition_data/process/facility_master.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, str_schema_name, str_table_name, df)

    # station master
    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'station_master'

    df = FileUtil.get_instance().load_parquet('/app/competition_data/process/station_master.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, str_schema_name, str_table_name, df)

    # mesh data
    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'mesh_data'

    df = FileUtil.get_instance().load_parquet('/app/competition_data/process/mesh_data.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, str_schema_name, str_table_name, df)

    # price country data
    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'price_country_data'

    df = FileUtil.get_instance().load_parquet('/app/competition_data/process/price_country_data.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, str_schema_name, str_table_name, df)

    # price prefecture data
    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'price_prefecture_data'

    df = FileUtil.get_instance().load_parquet('/app/competition_data/process/price_prefecture_data.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, str_schema_name, str_table_name, df)

    # dbt_train
    lst_subprocess = [
        'dbt',
        'run',
        '--profiles-dir',
        '/app/dbt/dbt_profiles_postgresql',
        '--project-dir',
        '/app/dbt/dbt_projects_postgresql/make_prepare_datamart',
        '--vars',
        '{ train_test: train }'
    ]

    subprocess.run(lst_subprocess)

    # dbt_test
    lst_subprocess = [
        'dbt',
        'run',
        '--profiles-dir',
        '/app/dbt/dbt_profiles_postgresql',
        '--project-dir',
        '/app/dbt/dbt_projects_postgresql/make_prepare_datamart',
        '--vars',
        '{ train_test: test }'
    ]

    subprocess.run(lst_subprocess)

