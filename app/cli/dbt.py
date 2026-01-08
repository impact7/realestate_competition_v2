import logging
import subprocess
from app.modules.settings.config import AppConfig
from app.modules.utils.logger import setup_logging

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

    # dbt_train
    lst_subprocess = [
        'dbt',
        'run',
        '--profiles-dir',
        '/app/dbt/dbt_profiles_postgresql',
        '--project-dir',
        '/app/dbt/dbt_projects_postgresql/make_prepare_datamart',
        '--vars',
        '{train_test: train}'
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

