import logging
import subprocess
from app.modules.settings.config import AppConfig
from app.modules.utils.logger import setup_logging
from app.modules.utils.fileutil import FileUtil
from app.modules.utils.db_client import DBClient
from app.modules.prepare_data.upload_parquet_to_db.upload_parquet_to_db import UploadParquetToDB


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
    logger.info('submit_task')

    db_client = DBClient.create(**dict_db)
    db_client.execute_sql('create schema if not exists output')

    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'test_predict2'

    df = FileUtil.get_instance().load_parquet('/app/parquet/test_predict/test_predict.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, 'output', str_table_name, df)

    df = db_client.select_sql('select * from output.test_predict2 limit 50')


    # dbt
    lst_subprocess = [
        'dbt',
        'run',
        '--profiles-dir',
        '/app/dbt/dbt_profiles_postgresql',
        '--project-dir',
        '/app/dbt/dbt_projects_postgresql/make_submit_datamart',
    ]

    subprocess.run(lst_subprocess)

    db_client = DBClient.create(**dict_db)
    df = db_client.select_sql('select * from submit_data.\"12_01_test_predict\"')

    FileUtil.get_instance().save_df_csv(df, '/app/output/submit.csv')
