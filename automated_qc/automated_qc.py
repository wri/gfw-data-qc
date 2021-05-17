import os
import fiona
import argparse
import numpy as np
import rasterio as rio
from utils import *
from gcs import list_gcs_assets
from osgeo import gdal
from datetime import date
from rasterio.windows import from_bounds
from rasterstats import zonal_stats

# config parser
parser = argparse.ArgumentParser(description='Calculate RADD alerts in features')
parser.add_argument('shp', type=str, help='path to shapefile')
parser.add_argument('--layers', type=str, help='data-api contextual layers (technical titles), separated by commas', default=' ')
parser.add_argument('--output', type=str, help='path to output directory', defualt='out')
parser.add_argument('--start', type=int, help='start date in YYYYMMDD format')
parser.add_argument('--end', type=int, help='END date in YYYYMMDD format')
args = parser.parse_args()
shp_fp = args.shp
contextual_layers = args.layers.replace(' ', '').split(',')
out_dir = args.output

# data config
DATA_DIR = '../../data'
IN_DIR = os.path.join(DATA_DIR, 'admin_areas')
OUT_DIR = os.path.join(DATA_DIR, 'output')
SHP_FN = 'ituri.shp'
contextual_layers = []
start_date = date(2019, 1, 1)
end_date = date(2021, 4, 15)

shp_fp = os.path.join(IN_DIR, SHP_FN)

# create directories
if not os.path.exists('vrt'):
    os.mkdir('vrt')

# set GCS credentials - assumes GCS credentials json file is in directory- TODO: generate this JSON w boto3
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(os.getcwd(), 'gcs_config.json')
gdal.SetConfigOption('GOOGLE_APPLICATION_CREDENTIALS', os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'))

# list RADD TIFFs
latest_alerts = list_gcs_assets(
    'gfw_gee_export',
    'wur_radd_alerts/v20210425',
    os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
)

# build VRT of latest alerts
vrt_path = os.path.join('vrt', 'radd.vrt')
if not os.path.exists(vrt_path):
    vrt = gdal.BuildVRT(vrt_path, latest_alerts)
    vrt.FlushCache()

# read bounding box of shp
with fiona.open(shp_fp) as src:
    bounds = src.bounds

# read window
with rio.open('radd.vrt') as src:
    window = from_bounds(
        bounds[0],
        bounds[1],
        bounds[2],
        bounds[3],
        src.transform
    )
    arr = src.read(1,window=window)
    win_affine = src.window_transform(window)

# remove confidence encoding
arr[arr >= 30000] -= 30000
arr[arr >= 20000] -= 20000

# filer by date
orig_date = date(2014, 12, 31)
max_date = end_date - orig_date
min_date = start_date - orig_date
arr_mask = arr.copy()
arr_mask[(arr > min_date.days) & (arr < max_date.days)] = 1
arr_mask[np.where(arr_mask != 1 )] = 0

# intersect/dissolve features
feature = intersect_layers(bounds=bounds, shp_fp=shp_fp)

# sum number of alerts
zstats = zonal_stats(
    shp_fp,
    arr_mask,
    stats='sum',
    affine=win_affine,
    all_touched=False,
    nodata = 999
)
