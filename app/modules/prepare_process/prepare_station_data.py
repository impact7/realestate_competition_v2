import geopandas as gpd
import subprocess
from app.modules.utils.fileutil import FileUtil

class PrepareStationData:
    def __init__(self):
        pass

    @classmethod
    def create(cls) -> "PrepareStationData":
        return PrepareStationData()

    def prepare_data(self, str_parquet_file_path: str):

        subprocess.run([
            'unzip',
            '-o',
            '/app/competition_data/S12-24_GML.zip',
            '-d',
            '/app/competition_data/'
        ])

        str_station_file_path = '/app/competition_data/UTF-8/S12-24_NumberOfPassengers.geojson'

        gdf = FileUtil.get_instance().read_geojson(str_station_file_path)
        gdf = gdf.explode(ignore_index=True)

        gdf['lon'] = gdf.geometry.explode(ignore_index=True).centroid.x
        gdf['lat'] = gdf.geometry.explode(ignore_index=True).centroid.y

        gdf = gdf.drop(['geometry'], axis=1)

        lst_column_names = [column_name.lower() for column_name in gdf.columns.tolist()]
        gdf.columns = lst_column_names

        FileUtil.get_instance().save_parquet(gdf, str_parquet_file_path)
