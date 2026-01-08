from typing import Dict

from app.modules.utils.fileutil import FileUtil
from app.modules.settings.settings import CONFIG_APP_PATH, CONFIG_TEST_PATH
import yaml

class AppConfig(object):
    instance = None

    # __new()__をオーバーライド
    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(cls)

        return cls.instance

    def __init__(self, str_app_type: str = 'app'):
        if not hasattr(self, 'str_app_type'):
            self.str_app_type = str_app_type

    @classmethod
    def get_instance(cls, str_app_type: str = 'app') -> 'AppSettings':
        if cls.instance is None:
            cls.instance = cls(str_app_type)

        return cls.instance

    @classmethod
    def reset_instance(cls):
        cls.instance = None


    def get_app_type(self) -> str:
        return self.str_app_type

    def get_config(self) -> Dict:
        if self.str_app_type == 'app':
            dict_config = FileUtil.get_instance().load_yaml(CONFIG_APP_PATH)

            return dict_config
        elif self.str_app_type == 'test':
            dict_config = FileUtil.get_instance().load_yaml(CONFIG_TEST_PATH)

            return dict_config
        else:
            raise NotImplementedError()


