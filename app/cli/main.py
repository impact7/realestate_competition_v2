import logging
import subprocess
from app.modules.settings.config import AppConfig
from app.modules.utils.logger import setup_logging
from app.modules.utils.fileutil import FileUtil
from app.modules.utils.seed import SeedUtil
from app.modules.settings.settings import DEFAULT_SEED
from app.modules.datamart.cv_ml_datamart import CVMLDatamart
from app.modules.utils.db_client import DBClient
from app.modules.cv_models.cv_models import CVModels
from app.modules.metrics.metrics_builder import MetricsBuilder
from app.modules.test_predict.make_test_predict import MakeTestPredict
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

    SeedUtil.get_instance(DEFAULT_SEED).seed_everything()

    cv_datamart = CVMLDatamart()

    db_client = DBClient.create(**dict_db)
    db_client.execute_sql('create schema if not exists output')

    db_client = DBClient.create(**dict_db)
    df = db_client.select_sql('select * from prepare_data.\"04_15_train_dm\"')

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

    cv_datamart.make_cv_ml_datamart(df, str_target_column_name,
                                       lst_feature_column_names,
                                       lst_drop_column_names,
                                       lst_info_column_names,
                                       lst_id_column_names,
                                       lst_group_column_names,
                                       n_folds,
                                       fold_no_flag,
                                       log1p_flag,
                                       adjust_amount)

#    FileUtil.get_instance().save_pickle(cv_datamart, '/app/json/cv_datamart.pickle')

    dict_model_params = FileUtil.get_instance().load_json('/app/json/model_params.json')

    cv_models = CVModels()
    cv_datamart = cv_models.build_cv_models(dict_model_params, cv_datamart)

    # Metricsの計算
    lst_y_test, lst_y_pred = cv_datamart.get_test_and_predict()
    metrics = MetricsBuilder.create_metrics(cv_models.get_model_name())
    dict_metrics = metrics.compute_metrics(lst_y_test, lst_y_pred)

    # model、datamart、metricsの保存
    FileUtil.get_instance().save_pickle(cv_models, '/app/pkl/cv_model.pkl')
    FileUtil.get_instance().save_pickle(cv_datamart, '/app/pkl/cv_datamart.pkl')
    FileUtil.get_instance().save_json(dict_metrics, '/app/json/model_metrics/metrics.json')

    df = db_client.select_sql('select * from prepare_data.\"04_15_test_dm\"')

    lst_feature_column_names = None
    lst_drop_column_names = ["id"]
    lst_id_column_names = ["id"]

    make_test_predict = MakeTestPredict.create()
    df_pred = make_test_predict.make_test_predict(cv_models,
                                                  df,
                                                  lst_feature_column_names,
                                                  lst_drop_column_names,
                                                  lst_id_column_names)

    FileUtil.get_instance().save_parquet(df_pred, '/app/parquet/test_predict/test_predict.parquet')

    # test_predict
    upload_parquet_to_db = UploadParquetToDB.create()

    str_table_name = 'test_predict'

    df = FileUtil.get_instance().load_parquet('/app/parquet/test_predict/test_predict.parquet')

    upload_parquet_to_db.upload_parquet_to_db(dict_db, 'output', str_table_name, df)

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

    df = db_client.select_sql('select * from submit_data.\"10_01_test_predict\"')

    FileUtil.get_instance().save_df_csv(df, '/app/output/submit.csv')


