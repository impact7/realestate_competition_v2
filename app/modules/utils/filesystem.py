from typing import List
import glob
import s3fs
import gcsfs
import os
import logging
import pandas as pd
from typing import Dict
from abc import abstractmethod

logger = logging.getLogger(__name__)

class FileSystem:
    def __init__(self):
        pass

    @classmethod
    def create(cls, str_file_path: str, dict_s3_params: Dict = None) -> 'FileSystem':
        if str_file_path.startswith('s3://'):
            return S3FileSsytem(dict_s3_params)
        elif str_file_path.startswith('gs://'):
            return GCSFileSystem()
        else:
            return LocalFileSytem()

    @abstractmethod
    def open(self, str_file_path: str, mode: str = 'r', encoding: str = 'utf-8'):
        raise NotImplementedError()

    @abstractmethod
    def glob(self, str_glob_path: str) -> List:
        raise NotImplementedError()

    @abstractmethod
    def exists(self, str_file_path: str) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def makedirs(self, str_folder_path: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def to_parquet(self, df: pd.DataFrame, str_file_path: str):
        raise NotImplementedError()

    @abstractmethod
    def read_parquet(self, str_file_path: str) -> pd.DataFrame:
        raise NotImplementedError()


class LocalFileSytem(FileSystem):
    def __init__(self):
        super().__init__()

    def open(self, str_file_path: str, mode: str = 'r', encoding: str = None):
        if encoding is None:
            return open(str_file_path, mode=mode)
        else:
            return open(str_file_path, mode=mode, encoding=encoding)

    def glob(self, str_glob_path: str) -> List:
        return glob.glob(str_glob_path)

    def exists(self, str_file_path: str) -> bool:
        return os.path.exists(str_file_path)

    def makedirs(self, str_folder_path: str) -> None:
        os.makedirs(str_folder_path, exist_ok=True)

    def to_parquet(self, df: pd.DataFrame, str_file_path: str):
        df.to_parquet(str_file_path, index=False)

    def read_parquet(self, str_file_path: str) -> pd.DataFrame:
        return pd.read_parquet(str_file_path)


class S3FileSsytem(FileSystem):
    def __init__(self, dict_s3_params: Dict):
        super().__init__()
        self.dict_s3_params = dict_s3_params
        self.fs = s3fs.S3FileSystem(**dict_s3_params)

    def open(self, str_file_path: str, mode: str = 'r', encoding: str = None):
        if encoding is None:
            return self.fs.open(str_file_path, mode)
        else:
            return self.fs.open(str_file_path, mode, encoding)

    def glob(self, str_glob_path: str) -> List:
        lst_files = self.fs.glob(str_glob_path)
        lst_files = ['s3://{}'.format(str_file_path) for str_file_path in lst_files]

        return lst_files

    def exists(self, str_file_path: str) -> bool:
        return self.fs.exists(str_file_path)

    def makedirs(self, str_folder_path: str) -> None:
        self.fs.makedirs(str_folder_path)

    def to_parquet(self, df: pd.DataFrame, str_file_path: str):
        df.to_parquet(str_file_path, index=False,
                      storage_options=self.dict_s3_params)

    def read_parquet(self, str_file_path: str) -> pd.DataFrame:
        return pd.read_parquet(str_file_path,
                               storage_options=self.dict_s3_params)

class GCSFileSystem(FileSystem):
    def __init__(self):
        super().__init__()
        self.fs = gcsfs.GCSFileSystem()

    def open(self, str_file_path: str, mode: str = 'r', encoding: str = None):
        if encoding is None:
            return self.fs.open(str_file_path, mode)
        else:
            return self.fs.open(str_file_path, mode, encoding)

    def glob(self, str_glob_path: str) -> List:
        lst_files = self.fs.glob(str_glob_path)
        lst_files = ['gs://{}'.format(str_file_path) for str_file_path in lst_files]

        return lst_files

    def exists(self, str_file_path: str) -> bool:
        return self.fs.exists(str_file_path)

    def makedirs(self, str_folder_path: str) -> None:
        self.fs.makedirs(str_folder_path)


