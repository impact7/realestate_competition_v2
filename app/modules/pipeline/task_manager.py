from typing import Dict
from app.modules.pipeline.task_builder import TaskBuilder
from app.modules.utils.fileutil import FileUtil

class TaskManager:
    def __init__(self, dict_project_info: Dict):
        self.dict_project_info = dict_project_info

    def execute_jobs(self, dict_tasks: Dict) -> None:
        for key, dict_task in dict_tasks.items():
            task = TaskBuilder.create_task(dict_task)

            # s3_paramsが定義されている場合は更新し、定義されていない場合は削除する
            if 's3_params' in dict_task:
                FileUtil.get_instance().set_s3_params(dict_task['s3_params'])
            else:
                FileUtil.get_instance().clear_s3_params()

            task.execute(dict_task)

