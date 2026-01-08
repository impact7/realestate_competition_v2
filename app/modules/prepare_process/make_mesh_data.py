import pandas as pd
import subprocess
import glob
import geopandas as gpd
from app.modules.utils.fileutil import FileUtil

class MakeMeshData:
    def __init__(self):
        pass

    @classmethod
    def create(cls) -> "MakeMeshData":
        return MakeMeshData()

    def prepare_data(self, str_parquet_file_path: str):

        subprocess.run([
            'unzip',
            '-o',
            '/app/competition_data/250m_mesh_2024_GEOJSON.zip',
            '-d',
            '/app/competition_data/'
        ])

        lst_df = []

        for i in range(1, 48, 1):
            subprocess.run([
                'unzip',
                '-o',
                '/app/competition_data/250m_mesh_2024_{:02d}_GEOJSON.zip'.format(i),
                '-d',
                '/app/competition_data/'
            ])

            str_file_path = '/app/competition_data/250m_mesh_2024_{:02d}.geojson'.format(
                i, i
            )

            lst_df.append(self._make_df(str_file_path))

        df = pd.concat(lst_df, axis=0)

        df = df.drop_duplicates().reset_index(drop=True)

        FileUtil.get_instance().save_parquet(df, str_parquet_file_path)

    def _make_df(self, str_file_path: str):
        df = gpd.read_file(str_file_path)

        df = df.explode(ignore_index=True)

        def polygon_to_coords(poly):
            return list(poly.exterior.coords)

        df['coords'] = df.geometry.apply(polygon_to_coords)

        df['left_lon'] = df.apply(lambda row: row['coords'][0][0], axis=1)
        df['bottom_lat'] = df.apply(lambda row: row['coords'][0][1], axis=1)
        df['right_lon'] = df.apply(lambda row: row['coords'][2][0], axis=1)
        df['top_lat'] = df.apply(lambda row: row['coords'][2][1], axis=1)

        df = df.loc[:, ['MESH_ID', 'PTN_2020', 'PT00_2025', 'PT00_2030', 'PT00_2050', 'PT00_2070', 'left_lon',
                        'bottom_lat', 'right_lon', 'top_lat']]

        df.columns = ['mesh_code', 'pt2020', 'pt2025', 'pt2030', 'pt2050', 'pt2070',
                      'left_lon', 'bottom_lat', 'right_lon', 'top_lat']

        return df

