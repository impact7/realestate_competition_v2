import logging
import argparse
from app.modules.settings.config import AppConfig

dict_config = AppConfig.get_instance().get_config()
logger = logging.getLogger(__name__)

class ArgsPipeline(object):
    def __init__(self):
        pass

    @classmethod
    def create(cls):
        return ArgsPipeline()

    def get_pipeline_file_path(self) -> str:
        arg_parser = argparse.ArgumentParser()

        arg_parser.add_argument('pipeline_file_path', type=str)

        args = arg_parser.parse_args()

        str_pipeline_file_path = args.pipeline_file_path

        logger.info('Function Type: {}'.format(str_pipeline_file_path))

        return str_pipeline_file_path


