import os
import math
import numpy as np
import rasterio as rio
import fiona
import pandas as pd
import argparse
import requests
import geopandas as gpd
from utils import parse_bounds, concatenate_windows, get_s3_asset_uri
from tqdm import tqdm
from rasterio.windows import from_bounds
from rasterstats import zonal_stats

# parse arguments
parser = argparse.ArgumentParser(description='Calculate tree cover loss for admin area')
parser.add_argument('--shp', type=str, help='path to admin area shapefile')
parser.add_argument('--layers', type=str, help='contextual layers to intersect with admin area, separated by commas')
parser.add_argument('--dissolve', type=bool, help='True or False for dissolving shapefile intersection with contextual layers', default=True)
parser.add_argument('--out', type=str, help='path to output directory', default='out')
args = parser.parse_args()
shp_fp = args.shp
contextual_layers = args.layers.split(',')
dissolve = args.dissolve
OUT_DIR = args.out

# create directories
if not os.path.exists(OUT_DIR):
    os.mkdir(OUT_DIR)

if not os.path.exists('tmp'):
    os.mkdir('tmp')

# get admin area bounds
with fiona.open(shp_fp) as src:
    bounds = src.bounds
X_list, Y_list = parse_bounds(bounds)

def intersect_layers(layers, bounds, shp_fp, dissolve):

    # return dissolved shapefile if there are no contextual layers
    if len(contextual_layers) == 0:
        adm_shp = gpd.read_file(shp_fp)
        if dissolve==True:
            dissolved = gpd.GeoSeries(adm_shp.geometry).unary_union
            dissolved_gs = gpd.GeoSeries(dissolved)
            dissolved_gs.to_file(os.path.join('tmp', f'{os.path.basename(shp_fp[:-4])}_tmp.shp'))
            return dissolved
        else:
            adm_shp.to_file(os.path.join('tmp', f'{os.path.basename(shp_fp[:-4])}_tmp.shp'))
            return adm_shp

    # parse for s3 paths
    s3_paths = [get_s3_asset_uri(layer) for layer in layers]

    # read contextual layers within bounds of admin area
    contextual_gdfs = []
    for s3_path in s3_paths:
        if s3_path[-4:] == '.zip':
            filename=f'zip+{s3_path}'
        else:
            filename=s3_path
        gdf = gpd.read_file(
            filename=filename,
            bbox=bounds,
        )
        if len(gdf) > 0:
            contextual_gdfs.append(gdf)
        else:
            continue

    if len(contextual_gdfs) == 0:
        raise NoIntersectException('Shapefile does not intersect with contextual layers')

    # intersect all layers
    intersected_gdf = gpd.read_file(shp_fp)
    for contextual_gdf in contextual_gdfs:
        try:
            intersected_gdf = gpd.overlay(intersected_gdf, contextual_gdf, how='intersection')
        except:
            continue

# generate area arrays
area_arrs = []
for Y in Y_list:
    for X in X_list:
        with rio.open(f's3://gfw-files/2018_update/area/{Y}_{X}.tif') as src:
            window = from_bounds(
                bounds[0],
                bounds[1],
                bounds[2],
                bounds[3],
                src.transform
            )
            area_arrs.append(src.read(1, window=window))
            win_affine = src.window_transform(window)
        
        print(f'Extracted window for {Y}, {X}')

# contactenate area arrays
area_arr = concatenate_windows(area_arrs, X_list, Y_list)

# mask by tree cover gain
tcg_arrs = []
for Y in Y_list:
    for X in X_list:
        with rio.open(f's3://gfw-data-lake/umd_tree_cover_gain_from_height/v202206/raster/epsg-4326/10/40000/gain/geotiff/{Y}_{X}.tif') as src:
            tcl_arr = src.read(1, window=from_bounds(
                bounds[0],
                bounds[1],
                bounds[2],
                bounds[3],
                src.transform
        ))
        
        tcg_arrs.append(tcl_arr)
        print(f'Extracted window for tree cover loss tile: {Y}, {X}')      

tcg_arr = concatenate_windows(tcg_arrs, X_list, Y_list)
tcg_masked = np.multiply(area_arr, tcg_arr)

# calculate zonal stats
zstats = zonal_stats(
    os.path.join('tmp', f'{os.path.basename(shp_fp[:-4])}_dissolved.shp'),
    tcg_masked / 10000,
    stats='sum',
    affine=win_affine,
    all_touched=False,
    nodata = 999
)
int(zstats[0]['sum'])