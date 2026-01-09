import logging
import subprocess
from app.modules.settings.config import AppConfig
from app.modules.utils.logger import setup_logging
from app.modules.utils.fileutil import FileUtil
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
    logger.info('submit_task')

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
