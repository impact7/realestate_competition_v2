import ast
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class SafeDict(dict):
    def __missing__(self, key):
        # 欠損キーをそのまま残す
        return '{' + key + '}'

class DictTaskTool:
    @classmethod
    def merge_common(cls, dict_tasks: Dict, dict_common: Dict):
        for task_name, dict_task in dict_tasks.items():
            for key, item in dict_common.items():
                # dict_commonに新しいデータがある場合のみ追加
                if dict_tasks[task_name].get(key) is None:
                    dict_tasks[task_name][key] = item

        return dict_tasks

    @classmethod
    def parameter_convert(cls, dict_tasks: Dict, dict_parameters: Dict, dict_parameters_with_parameters: Dict,
                          dict_dict_parameters: Dict) -> Dict:

        dict_parameters = cls._make_parameters_by_parameters_with_parameters(dict_parameters,
                                                                             dict_parameters_with_parameters)

        logger.info(f'dict_parameters: {dict_parameters}')

        if dict_parameters is not None:
            dict_tasks = cls._string_parameter_convert(dict_tasks, dict_parameters)

        # dict_parametersの中も置換
        if dict_dict_parameters is not None and dict_parameters is not None:
            dict_dict_parameters = cls._string_parameter_convert(dict_dict_parameters, dict_parameters)

        if dict_dict_parameters is not None:
            dict_tasks = cls._dict_parameter_convert(dict_tasks, dict_dict_parameters)

        return dict_tasks

    @classmethod
    def _make_parameters_by_parameters_with_parameters(self, dict_parameters: Dict,
                                                       dict_parameters_with_parameters: Dict) -> Dict:
        if dict_parameters_with_parameters is None:
            return dict_parameters

        for key, item in dict_parameters_with_parameters.items():
            if dict_parameters.get(key) is None:
                if isinstance(item, str):
                    dict_parameters[key] = item.format_map(SafeDict(dict_parameters))

        return dict_parameters

    @classmethod
    def _string_parameter_convert(cls, dict_tasks: Dict, dict_parameters: Dict) -> Dict:
        if dict_parameters is None:
            return dict_tasks

        for task_name, dict_task in dict_tasks.items():
            for key, item in dict_task.items():
                if isinstance(item, str):
                    dict_tasks[task_name][key] = item.format_map(SafeDict(dict_parameters))
                elif isinstance(item, dict):
                    for child_key, child_item in item.items():
                        if isinstance(child_item, str):
                            dict_tasks[task_name][key][child_key] = child_item.format_map(SafeDict(dict_parameters))

        return dict_tasks

    @classmethod
    def _dict_parameter_convert(cls, dict_tasks: Dict, dict_dict_parameters: Dict) -> Dict:
        if dict_dict_parameters is None:
            return dict_tasks

        for task_name, dict_task in dict_tasks.items():
            for key, item in dict_task.items():
                if isinstance(item, str):
                    str_before = item
                    str_after = item.format_map(SafeDict(dict_dict_parameters))

                    if str_before != str_after:
                        dict_tasks[task_name][key] = ast.literal_eval(str_after)

                elif isinstance(item, dict):
                    for child_key, child_item in item.items():
                        if isinstance(child_item, str):
                            str_before = child_item
                            str_after = child_item.format_map(SafeDict(dict_dict_parameters))

                            if str_before != str_after:
                                dict_tasks[task_name][key][child_key] = ast.literal_eval(str_after)

        return dict_tasks