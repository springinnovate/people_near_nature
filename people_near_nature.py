"""Calcualte people near nature."""
import logging
import os
import math
import sys

from osgeo import gdal
from osgeo import osr
import ecoshard
import numpy
import pygeoprocessing
import taskgraph
import taskgraph_downloader_pnn

WORKSPACE_DIR = 'people_near_nature_workspace'
CHURN_DIR = os.path.join(WORKSPACE_DIR, 'churn')
ALIGNED_DIR = os.path.join(WORKSPACE_DIR, 'aligned')
ECOSHARD_DIR = os.path.join(WORKSPACE_DIR, 'ecoshard')
TASKGRAPH_DIR = os.path.join(WORKSPACE_DIR, 'taskgraph')
ECOSHARD_BASE_URL = 'https://storage.googleapis.com/critical-natural-capital-ecoshards/'

POPULATION_URL = (
    ECOSHARD_BASE_URL +
    'lspop2017_md5_faaad64d15d0857894566199f62d422c.zip')
HAB_MASK_URL = (
    ECOSHARD_BASE_URL +
    'masked_nathab_esa_nodata_md5_7c9acfe052cb7bdad319f011e9389fb1.tif')
POOR_POP_URL = (
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
    for dir_path in [
            WORKSPACE_DIR, CHURN_DIR, ECOSHARD_DIR, TASKGRAPH_DIR, ALIGNED_DIR]:
        try:
            os.makedirs(dir_path)
        except OSError:
            pass
    task_graph = taskgraph.TaskGraph(TASKGRAPH_DIR, 4)
    tg_downloader = taskgraph_downloader_pnn.TaskGraphDownloader(
        ECOSHARD_DIR, task_graph)

    for url, key in [
            (POOR_POP_URL, 'poor_pop'),
            (HAB_MASK_URL,  'hab_mask')]:
        tg_downloader.download_ecoshard(url, key)
    tg_downloader.download_ecoshard(
        POPULATION_URL, 'total_pop', 'unzip', 'lspop2017')

    # align rasters
    base_raster_list = [
        tg_downloader.get_path(key) for key in [
            'hab_mask', 'total_pop', 'poor_pop']]

    aligned_raster_list = [
        os.path.join(ALIGNED_DIR, os.path.basename(path))
        for path in base_raster_list]

    target_pixel_size = pygeoprocessing.get_raster_info(
        tg_downloader.get_path('total_pop'))['pixel_size']

    align_raster_task = task_graph.add_task(
        func=pygeoprocessing.align_and_resize_raster_stack,
        args=(
            base_raster_list, aligned_raster_list, ['near'] * 3,
            target_pixel_size, 'intersection'),
        target_path_list=aligned_raster_list,
        task_name='align rasters')

    aligned_path_map = {
        'total_pop': aligned_raster_list[1],
        'poor_pop': aligned_raster_list[2],
    }

    meters_per_degree = 110000.0
    for km_size in [10, 100]:
        degrees = km_size * 1000 / meters_per_degree
        pixels = max(1, int(degrees/target_pixel_size[0]))
        kernel_radius = (pixels, pixels)
        kernel_filepath = os.path.join(
            CHURN_DIR, '%d_kernel.tif' % km_size)
        kernel_task = task_graph.add_task(
            func=create_averaging_kernel_raster,
            args=(kernel_radius, kernel_filepath),
            kwargs={'normalize': False},
            target_path_list=[kernel_filepath],
            task_name='create %d kernel' % km_size)

        for population_key in ['total_pop', 'poor_pop']:
            population_spread_raster_path = os.path.join(
                CHURN_DIR, 'pop_spread_%s_%d.tif' % (population_key, km_size))
            spread_task = task_graph.add_task(
                func=pygeoprocessing.convolve_2d,
                args=(
                    (aligned_path_map[population_key], 1),
                    (kernel_filepath, 1), population_spread_raster_path),
                kwargs={'ignore_nodata': True},
                target_path_list=[population_spread_raster_path],
                dependent_task_list=[kernel_task, align_raster_task],
                task_name='spread %s to %d' % (population_key, km_size))

            pop_on_hab_raster_path = os.path.join(
                WORKSPACE_DIR, '%s_%d.tif' % (population_key, km_size))

            # aligned_raster_list[0] is the hab mask
            spread_nodata = -1
            task_graph.add_task(
                func=build_overviews_raster_calculator,
                args=(
                    [(population_spread_raster_path, 1),
                     (aligned_raster_list[0], 1)], mask_op,
                    pop_on_hab_raster_path, gdal.GDT_Float32, spread_nodata),
                target_path_list=[pop_on_hab_raster_path],
                dependent_task_list=[spread_task],
                task_name='mask %s %d' % (population_key, km_size))

    task_graph.join()
    task_graph.close()


def build_overviews_raster_calculator(
        base_raster_path_band_const_list, local_op, target_raster_path,
        datatype_target, nodata_target):
    """Passthrough for raster_calculator."""
    local_raster_nodata_list = [
        (pygeoprocessing.get_raster_info(path[0])['nodata'][0], 'raw')
        for path in base_raster_path_band_const_list]
    pygeoprocessing.raster_calculator(
        base_raster_path_band_const_list + local_raster_nodata_list, local_op,
        target_raster_path, datatype_target, nodata_target)
    ecoshard.build_overviews(target_raster_path)


def mask_op(signal, mask, mask_nodata, target_nodata):
    result = numpy.empty_like(signal)
    result[:] = target_nodata
    valid_mask = ~numpy.isclose(mask, mask_nodata)
    result[valid_mask] = signal[valid_mask]
    return result

    # # make a 10km kernel
    # # make a 100km kernel

    # utm_lng_step = 360//60
    # degree_buffer = 1.0

    # wgs84_srs = osr.SpatialReference()
    # wgs84_srs.ImportFromEPSG(4326)
    # wgs84_wkt = wgs84_srs.ExportToWkt()

    # for lng in range(-180, 180, 360//60):
    #     for lat_code in [6, 7]:
    #         utm_code = (math.floor((lng+180)/6) % 60) + 1
    #         epsg_utm_code = int('32%d%02d' % (lat_code, utm_code))
    #         utm_srs = osr.SpatialReference()
    #         utm_srs.ImportFromEPSG(epsg_utm_code)
    #         utm_wkt = utm_srs.ExportToWkt()

    #         LOGGER.debug('%d epsg:%s', lng, epsg_utm_code)
    #         wgs84_bounding_box = [
    #             lng-degree_buffer,
    #             (0 if lat_code == 6 else -80)-degree_buffer,
    #             lng+utm_lng_step+degree_buffer,
    #             (80 if lat_code == 6 else 0)+degree_buffer]
    #         LOGGER.debug(wgs84_bounding_box)

    #         utm_bounding_box = pygeoprocessing.transform_bounding_box(
    #             wgs84_bounding_box, wgs84_wkt, utm_wkt)

    #         for raster_key in [
    #                 'total_pop', 'poor_pop', 'hab_mask']:
    #             utm_raster_path = os.path.join(
    #                 CHURN_DIR, '%s_%s.tif' % (raster_key, epsg_utm_code))
    #             target_pixel_size = [1000.0, -1000.0]
    #             task_graph.add_task(
    #                 func=pygeoprocessing.warp_raster,
    #                 args=(
    #                     tg_downloader.get_path(raster_key),
    #                     target_pixel_size, utm_raster_path,
    #                     'near'),
    #                 kwargs={
    #                     'target_bb': utm_bounding_box,
    #                     'target_sr_wkt': utm_wkt},
    #                 target_path_list=[utm_raster_path],
    #                 task_name='project %s to %s' % (
    #                     utm_raster_path, epsg_utm_code))
    # tg_downloader.join()
    # task_graph.join()
    # task_graph.close()


def create_averaging_kernel_raster(
        radius_in_pixels, kernel_filepath, normalize=True):
    """Create a flat raster kernel with a 2d radius given.

    Parameters:
        radius_in_pixels (tuple): the (x/y) distance of the averaging kernel.
        kernel_filepath (string): The path to the file on disk where this
            kernel should be stored.  If this file exists, it will be
            overwritten.

    Returns:
        None

    """
    driver = gdal.GetDriverByName('GTiff')
    LOGGER.debug(radius_in_pixels)
    kernel_raster = driver.Create(
        kernel_filepath, int(2*radius_in_pixels[0]),
        int(2*radius_in_pixels[1]), 1, gdal.GDT_Float32)

    # Make some kind of geotransform, it doesn't matter what but
    # will make GIS libraries behave better if it's all defined
    kernel_raster.SetGeoTransform([1, 0.1, 0, 1, 0, -0.1])
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    kernel_raster.SetProjection(srs.ExportToWkt())

    kernel_band = kernel_raster.GetRasterBand(1)
    kernel_band.SetNoDataValue(-9999)

    n_cols = kernel_raster.RasterXSize
    n_rows = kernel_raster.RasterYSize
    iv, jv = numpy.meshgrid(range(n_rows), range(n_cols), indexing='ij')

    cx = n_cols / 2.0
    cy = n_rows / 2.0

    kernel_array = numpy.where(
        ((cx-jv)**2 + (cy-iv)**2)**0.5 <= radius_in_pixels[0], 1.0, 0.0)
    LOGGER.debug(kernel_array)

    # normalize
    if normalize:
        kernel_array /= numpy.sum(kernel_array)
    kernel_band.WriteArray(kernel_array)
    kernel_band = None
    kernel_raster = None


if __name__ == '__main__':
    main()
