import logging
from typing import Dict
from app.modules.tasks.make_data_for_db_task import MakeDataForDBTask
from app.modules.tasks.upload_data_to_db_task import UploadDataToDBTask
from app.modules.tasks.dbt_task import DbtTask
from app.modules.tasks.make_cv_datamart_task import MakeCVDatamartTask
from app.modules.tasks.make_single_datamart_task import MakeSingleDatamartTask
from app.modules.tasks.build_cv_model_task import BuildCVModelTask
from app.modules.tasks.build_single_model_task import BuildSingleModelTask
from app.modules.tasks.parameter_tuning_task import ParameterTuningTask
from app.modules.tasks.save_best_result_task import SaveBestResultTask
from app.modules.tasks.make_shap_values_task import MakeShapValuesTask
from app.modules.tasks.make_predict_task import MakePredictTask
from app.modules.tasks.upload_parquet_to_db_task import UploadParquetToDBTask
from app.modules.tasks.make_shap_importance_task import MakeShapImportanceTask
from app.modules.tasks.mlflow_clear_log_task import MLFlowClearLogTask
from app.modules.tasks.generate_gemini_output_task import GenerateGeminiOutputTask
from app.modules.tasks.make_test_predict_task import MakeTestPredictTask
from app.modules.tasks.output_db_to_csv_task import OutputDBToCSVTask
from app.modules.tasks.prepare_data_definition_task import PrepareDataDefinitionTask
from app.modules.tasks.prepare_station_data_task import PrepareStationDataTask
from app.modules.tasks.prepare_mesh_data_task import PrepareMeshDataTask
from app.modules.tasks.prepare_price_data_task import PreparePriceDataTask
from app.modules.tasks.make_target_enc_cv_datamart_task import MakeCVDatamartTargetEncTask
from app.modules.tasks.task import Task

logger = logging.getLogger(__name__)

class TaskBuilder:
    def __init__(self):
        pass

    @classmethod
    def create_task(cls, dict_task : Dict) -> Task:
        if dict_task['job_name'] == MakeDataForDBTask.get_job_name():
            return MakeDataForDBTask()
        elif dict_task['job_name'] == UploadDataToDBTask.get_job_name():
            return UploadDataToDBTask()
        elif dict_task['job_name'] == DbtTask.get_job_name():
            return DbtTask()
        elif dict_task['job_name'] == MakeCVDatamartTask.get_job_name():
            return MakeCVDatamartTask()
        elif dict_task['job_name'] == MakeSingleDatamartTask.get_job_name():
            return MakeSingleDatamartTask()
        elif dict_task['job_name'] == BuildCVModelTask.get_job_name():
            return BuildCVModelTask()
        elif dict_task['job_name'] == BuildSingleModelTask.get_job_name():
            return BuildSingleModelTask()
        elif dict_task['job_name'] == ParameterTuningTask.get_job_name():
            return ParameterTuningTask()
        elif dict_task['job_name'] == SaveBestResultTask.get_job_name():
            return SaveBestResultTask()
        elif dict_task['job_name'] == MakeShapValuesTask.get_job_name():
            return MakeShapValuesTask()
        elif dict_task['job_name'] == MakePredictTask.get_job_name():
            return MakePredictTask()
        elif dict_task['job_name'] == UploadParquetToDBTask.get_job_name():
            return UploadParquetToDBTask()
        elif dict_task['job_name'] == MakeShapImportanceTask.get_job_name():
            return MakeShapImportanceTask()
        elif dict_task['job_name'] == MLFlowClearLogTask.get_job_name():
            return MLFlowClearLogTask()
        elif dict_task['job_name'] == GenerateGeminiOutputTask.get_job_name():
            return GenerateGeminiOutputTask()
        elif dict_task['job_name'] == OutputDBToCSVTask.get_job_name():
            return OutputDBToCSVTask()
        elif dict_task['job_name'] == MakeTestPredictTask.get_job_name():
            return MakeTestPredictTask()
        elif dict_task['job_name'] == PrepareDataDefinitionTask.get_job_name():
            return PrepareDataDefinitionTask()
        elif dict_task['job_name'] == PrepareStationDataTask.get_job_name():
            return PrepareStationDataTask()
        elif dict_task['job_name'] == PrepareMeshDataTask.get_job_name():
            return PrepareMeshDataTask()
        elif dict_task['job_name'] == PreparePriceDataTask.get_job_name():
            return PreparePriceDataTask()
        elif dict_task['job_name'] == MakeCVDatamartTargetEncTask.get_job_name():
            return MakeCVDatamartTargetEncTask()
        else:
            logger.error('Unknown job name: {}'.format(dict_task['job_name']))
            raise NotImplementedError()
