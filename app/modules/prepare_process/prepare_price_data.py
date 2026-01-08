import pandas as pd
import subprocess
import glob
import geopandas as gpd
from app.modules.utils.fileutil import FileUtil

class PreparePriceData:
    def __init__(self):
        pass

    @classmethod
    def create(cls) -> "PreparePriceData":
        return PreparePriceData()

    def prepare_data(self, str_country_parquet_file_path: str, str_prefecture_parquet_file_path: str):

        subprocess.run([
            'unzip',
            '-o',
            '/app/competition_data/L01-25_GML.zip',
            '-d',
            '/app/competition_data/'
        ])

        df = self._make_country_df('/app/competition_data/L01-25_GML/L01-25.geojson')

        FileUtil.get_instance().save_parquet(df, str_country_parquet_file_path)

        subprocess.run([
            'unzip',
            '-o',
            '/app/competition_data/L02-25_GML.zip',
            '-d',
            '/app/competition_data/'
        ])

        df = self._make_prefecture_df('/app/competition_data/L02-25.geojson')

        FileUtil.get_instance().save_parquet(df, str_prefecture_parquet_file_path)

    def _make_country_df(self, str_file_path: str):
        df = gpd.read_file(str_file_path)
        df = df.explode(ignore_index=True)

        df['lon'] = df.geometry.x
        df['lat'] = df.geometry.y

        lst_use_column_names = ['L01_001', 'L01_002', 'L01_003', 'L01_008', 'L01_009', 'L01_025',
                    'L01_097', 'L01_098', 'L01_099', 'L01_100', 'L01_101', 'L01_102', 'L01_103', 'L01_104', 'lat', 'lon']

        lst_column_names = ['addr', 'kubun', 'number', 'price', 'fluctuate_rate', 'address',
            'price2018', 'price2019', 'price2020', 'price2021', 'price2022', 'price2023', 'price2024', 'price2025', 'lat', 'lon']

        df = df.loc[:, lst_use_column_names]
        df.columns = lst_column_names

        return df

    def _make_prefecture_df(self, str_file_path: str):
        df = gpd.read_file(str_file_path)
        df = df.explode(ignore_index=True)

        df['lon'] = df.geometry.x
        df['lat'] = df.geometry.y

        lst_use_column_names = ['L02_020', 'L02_001', 'L02_002', 'L02_006', 'L02_007', 'L02_022',
            'L02_090', 'L02_091', 'L02_092', 'L02_093', 'L02_094', 'L02_095', 'L02_096', 'L02_097', 'lat', 'lon']

        lst_column_names = ['addr', 'kubun', 'number', 'price', 'fluctuate_rate', 'address',
            'price2018', 'price2019', 'price2020', 'price2021', 'price2022', 'price2023', 'price2024', 'price2025', 'lat', 'lon']

        df = df.loc[:, lst_use_column_names]
        df.columns = lst_column_names

        return df

