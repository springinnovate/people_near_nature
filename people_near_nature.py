"""Calcualte people near nature."""
import logging
import os
import math
import sys

import pygeoprocessing
import taskgraph_downloader_pnn

WORKSPACE_DIR = 'people_near_nature_workspace'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
TASKGRAPH_DIR = os.path.join(WORKSPACE_DIR, 'taskgraph')
ECOSHARD_BASE_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/'

POPULATION_URL = (
    ECOSHARD_BASE_URL +
    'lspop2017_md5_faaad64d15d0857894566199f62d422c.zip')
HAB_MASK_URL = (
    ECOSHARD_BASE_URL +
    'masked_nathab_esa_nodata_md5_7c9acfe052cb7bdad319f011e9389fb1.tif')
URBAN_POOR_POPULATION_URL = (
    ECOSHARD_BASE_URL +
    'rural_plus_urban_poor_pop_compressed_md5_e325640eb0ca7fdfaa5e6b3b31d2dc51.tif')

TOTAL_POP_10_OUTPUT_PATH = os.path.join(WORKSPACE_DIR, 'total_pop_10.tif')
TOTAL_POP_100_OUTPUT_PATH = os.path.join(WORKSPACE_DIR, 'total_pop_100.tif')
POOR_POP_10_OUTPUT_PATH = os.path.join(WORKSPACE_DIR, 'poor_pop_10.tif')
POOR_POP_100_OUTPUT_PATH = os.path.join(WORKSPACE_DIR, 'poor_pop_100.tif')

logging.basicConfig(
    level=logging.DEBUG,
    format=(
        '%(asctime)s (%(relativeCreated)d) %(levelname)s %(name)s'
        ' [%(pathname)s.%(funcName)s:%(lineno)d] %(message)s'),
    stream=sys.stdout)
LOGGER = logging.getLogger(__name__)
logging.getLogger('taskgraph').setLevel(logging.WARN)


def main():
    for dir_path in [WORKSPACE_DIR, CHURN_DIR, ECOSHARD_DIR, TASKGRAPH_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    tg_downloader = taskgraph_downloader_pnn.TaskGraphDownloader(
        ECOSHARD_DIR, TASKGRAPH_DIR)

    for url, key in [
            (URBAN_POOR_POPULATION_URL, 'urban_poor_population'),
            (HAB_MASK_URL,  'hab_mask')]:
        tg_downloader.download_ecoshard(url, key)
    tg_downloader.download_ecoshard(
        POPULATION_URL, 'population', 'unzip', 'lspop2017')

    utm_lng_step = 360//60
    degree_buffer = 1.0

    for lng in range(-180, 180, 360//60):
        for lat_code in [6, 7]:
            utm_code = (math.floor((lng+180)/6) % 60) + 1
            epsg_code = int('32%d%02d' % (lat_code, utm_code))
            LOGGER.debug('%d epsg:%s', lng, epsg_code)
            bounding_box = [
                lng-degree_buffer,
                (0 if lat_code == 6 else -80)-degree_buffer,
                lng+utm_lng_step+degree_buffer,
                (80 if lat_code == 6 else 0)+degree_buffer]
            LOGGER.debug(bounding_box)

    LOGGER.debug(tg_downloader.get_path('population'))
    LOGGER.debug(tg_downloader.get_path('urban_poor_population'))



    tg_downloader.join()

if __name__ == '__main__':
    main()
