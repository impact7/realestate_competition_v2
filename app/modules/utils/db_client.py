import psycopg2
from typing import Dict
from abc import abstractmethod
import pandas as pd
from app.modules.settings.config import AppConfig
from app.modules.utils.filesystem import FileSystem
from google.cloud import bigquery
from sqlalchemy import create_engine

dict_config = AppConfig.get_instance().get_config()

class DBClient:
    def __init__(self, dict_connect : Dict) -> None:
        self._dict_connect = dict_connect

    @abstractmethod
    def truncate_table(self, str_table_name : str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def drop_table(self, str_table_name : str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def copy_from_data(self, str_table_name: str, str_file_path: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def make_for_parquet(self, str_schema_name: str, str_table_name: str, df : pd.DataFrame):
        raise NotImplementedError()

    @abstractmethod
    def execute_sql(self, str_sql : str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def select_sql(self, str_sql: str) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def create(cls, dbms_name: str, conn: Dict = None) -> "DBClient":
        if dbms_name == 'PostgreSQL':
            return PostgreSQLClient(conn)
        elif dbms_name == 'BigQuery':
            return BigQuerySQLClient(conn)

        raise NotImplementedError()

    def _get_separator(self, str_file_path: str) -> str:
        if str_file_path.endswith('.csv'):
            return ','
        else:
            return '\t'


class PostgreSQLClient(DBClient):
    dict_connect : Dict

    def __init__(self, dict_connect : Dict) -> None:
        super().__init__(dict_connect)

    def drop_table(self, str_table_name : str) -> None:
        self.execute_sql('drop table if exists {}'.format(str_table_name))

    def copy_from_data(self, str_table_name: str, str_file_path: str) -> None:
        str_separator = self._get_separator(str_file_path)

        fs = FileSystem.create(str_file_path)

        with psycopg2.connect(**self._dict_connect) as conn:
            with conn.cursor() as cur:
                with fs.open(str_file_path, 'r') as f:
                    cur.copy_expert("COPY {} FROM STDIN delimiter '{}' csv header".format(str_table_name,
                                                                                          str_separator), f)

            conn.commit()

    def make_for_parquet(self, str_schema_name: str, str_table_name: str, df : pd.DataFrame):
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
            self._dict_connect['user'], self._dict_connect['password'], self._dict_connect['host'],
            self._dict_connect['port'], self._dict_connect['dbname']
        ))

        df.to_sql(str_table_name, engine, schema=str_schema_name,
                  if_exists='replace', index=False)

    def execute_sql(self, str_sql : str) -> None:
        """
        SQLを実行する関数
        :param str_sql: 実行するSQL
        """
        with psycopg2.connect(**self._dict_connect) as conn:
            with conn.cursor() as cur:
                cur.execute(str_sql)

            conn.commit()

    def select_sql(self, str_sql: str) -> pd.DataFrame:
        """
        SELECT文を実行する
        :param str_sql: 実行するSQL
        :return: 結果のデータフレーム
        """
        try:
            with psycopg2.connect(**self._dict_connect) as conn:
                df = pd.read_sql(str_sql, con=conn)

        except Exception as e:
            # テーブルが無いエラーの場合は空白を返す
            if 'does not exist' in str(e):
                df = pd.DataFrame()
            else:
                raise e

        return df

class BigQuerySQLClient(DBClient):
    dict_connect : Dict

    def __init__(self, dict_connect : Dict) -> None:
        super().__init__(dict_connect)

    def drop_table(self, str_table_name : str) -> None:
        self.execute_sql('drop table if exists {}'.format(str_table_name))

    def copy_from_data(self, str_table_name: str, str_file_path: str) -> None:
        client = bigquery.Client()

        str_separator = self._get_separator(str_file_path)

        # テーブルが存在することを前提に追記
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            field_delimiter=str_separator,
            write_disposition='WRITE_APPEND',
        )

        fs = FileSystem.create(str_file_path)

        with fs.open(str_file_path, 'rb') as f:
            job = client.load_table_from_file(f, str_table_name, job_config=job_config)

        job.result()

    def execute_sql(self, str_sql : str) -> None:
        """
        SQLを実行する関数
        :param str_sql: 実行するSQL
        """
        client = bigquery.Client()

        query_job = client.query(str_sql)
        query_job.result()

    def select_sql(self, str_sql: str) -> pd.DataFrame:
        """
        SELECT文を実行する
        :param str_sql: 実行するSQL
        :return: 結果のデータフレーム
        """
        client = bigquery.Client()
        df = client.query(str_sql).to_dataframe()

        return df

    def make_for_parquet(self, str_schema_name: str, str_table_name: str, df : pd.DataFrame):
        df.to_gbq('{}.{}'.format(str_schema_name, str_table_name), if_exists='replace')

