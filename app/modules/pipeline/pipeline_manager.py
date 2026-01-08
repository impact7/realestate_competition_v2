import logging
from typing import Dict
from app.modules.pipeline.task_manager import TaskManager
from app.modules.pipeline.dict_task_tool import DictTaskTool
from app.modules.settings.pipeline_setup import PipelineSetup

logger = logging.getLogger(__name__)

class PipelineManager:
    def __init__(self, dict_pipeline : Dict):
        self.dict_pipeline = dict_pipeline

    def execute(self):
        dict_project_info = self.dict_pipeline['project_info']
        dict_common = self.dict_pipeline['common_info']

        PipelineSetup.setup(
            dict_project_info.get('seed'),
        )

        dict_tasks = self.dict_pipeline['tasks']
        dict_tasks = DictTaskTool.merge_common(dict_tasks, dict_common)

        dict_tasks = DictTaskTool.parameter_convert(
            dict_tasks,
            dict_project_info.get('parameters'),
            dict_project_info.get('parameters_with_parameters'),
            dict_project_info.get('dict_parameters')
        )

#        logger.info(dict_tasks)

        task_manager = TaskManager(dict_project_info)
        task_manager.execute_jobs(dict_tasks)

