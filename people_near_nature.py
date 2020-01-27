"""Calcualte people near nature."""
import os

import ecoshard
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


def main(args):
    for dir_path in [WORKSPACE_DIR, CHURN_DIR, ECOSHARD_DIR, TASKGRAPH_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass

    tg_downloader = taskgraph_downloader_pnn.TaskGraphDownloader(
        ECOSHARD_DIR, TASKGRAPH_DIR)

    for url, key in [
            (POPULATION_URL,  'population'),
            (HAB_MASK_URL,  'hab_mask')]:
        tg_downloader.download_ecoshard(url, key)
    tg_downloader.download_ecoshard(
        URBAN_POOR_POPULATION_URL, 'urban_poor_population', 'unzip')

