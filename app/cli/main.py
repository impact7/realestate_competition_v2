import logging
from app.modules.settings.config import AppConfig
from app.modules.utils.logger import setup_logging
from app.modules.utils.fileutil import FileUtil
from app.modules.utils.seed import SeedUtil
from app.modules.settings.settings import DEFAULT_SEED
from app.modules.datamart.cv_ml_datamart import CVMLDatamart
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

    SeedUtil.get_instance(DEFAULT_SEED).seed_everything()

    cv_ml_datamart = CVMLDatamart()

    db_client = DBClient.create(**dict_db)
    df = db_client.select_sql('select * from prepare_data.\"04_31_train_dm\"')

    lst_feature_column_names = None
    lst_drop_column_names = ["id", "target", "money_room", "fold_no", "weight", "weight2", "weight3", "log_weight"]
    lst_info_column_names = None
    lst_id_column_names = ["id", "fold_no"]
    lst_group_column_names = None
    str_target_column_name = "target"
    n_folds = 5
    fold_no_flag = True
    log1p_flag = True
    adjust_amount = 1.0

    cv_ml_datamart.make_cv_ml_datamart(df, str_target_column_name,
                                       lst_feature_column_names,
                                       lst_drop_column_names,
                                       lst_info_column_names,
                                       lst_id_column_names,
                                       lst_group_column_names,
                                       n_folds,
                                       fold_no_flag,
                                       log1p_flag,
                                       adjust_amount)

    FileUtil.get_instance().save_pickle(cv_ml_datamart, '/app/json/cv_ml_datamart.pickle')
