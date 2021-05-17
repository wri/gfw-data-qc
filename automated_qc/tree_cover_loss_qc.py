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
parser.add_argument('--thresh', type=int, help='percent threshold for tree cover loss')
parser.add_argument('--layers', type=str, help='contextual layers to intersect with admin area, separated by commas')
parser.add_argument('--dissolve', type=bool, help='True or False for dissolving shapefile intersection with contextual layers', default=True)
parser.add_argument('--out', type=str, help='path to output directory', default='out')
args = parser.parse_args()
shp_fp = args.shp
treecover_threshold = args.thresh
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

    # dissolve and save to tmp
    if dissolve==True:
        dissolved = gpd.GeoSeries(intersected_gdf.geometry).unary_union
        dissolved_gs = gpd.GeoSeries(dissolved)
        dissolved_gs.to_file(os.path.join('tmp', f'{os.path.basename(shp_fp[:-4])}_tmp.shp'))
        return dissolved_gs
    else:
        return intersected_gdf

intersection = intersect_layers(contextual_layers, bounds, shp_fp)

# threshold tree cover density
print('Extracting tree cover density tile(s)')
tcd_arrs = []
for Y in Y_list:
    for X in X_list:
        with rio.open(f's3://gfw-data-lake/umd_tree_cover_density_2000/v1.6/raster/epsg-4326/10/40000/percent/geotiff/{Y}_{X}.tif') as src:
            window = from_bounds(
                bounds[0],
                bounds[1],
                bounds[2],
                bounds[3],
                src.transform
            )
            tcd_arrs.append(src.read(1, window=window))
            win_affine = src.window_transform(window)
            print(f'Extracted window for {Y}, {X}')

# concatenate if multiple windows
tcd_arr = concatenate_windows(tcd_arrs, X_list, Y_list)

# reclassify using binary threshold
print('Masking by treecover threshold')
tcd_arr_mask = tcd_arr.copy()
tcd_arr_mask[np.where( tcd_arr <= treecover_threshold )] = 0
tcd_arr_mask[np.where( tcd_arr > treecover_threshold )] = 1

# threshold area array
print('Extracting pixel area tile(s)')
area_arrs = []
for Y in Y_list:
    for X in X_list:
        with rio.open(f's3://gfw-files/2018_update/area/{Y}_{X}.tif') as src:
            area_arr = src.read(1, window=from_bounds(
                bounds[0],
                bounds[1],
                bounds[2],
                bounds[3],
                src.transform
            ))
        area_arrs.append(area_arr)
        print(f'Extracted window for {Y}, {X}')

# concatenate if multiple windows
area_arr = concatenate_windows(area_arrs, X_list, Y_list)

# mask by treecover threshold array
print('Masking by treecover threshold')
area_mask = np.multiply(tcd_arr_mask, area_arr)

# read as np array from bounds
print('Extracting tree cover loss tile(s)')
tcl_arrs = []
for Y in Y_list:
    for X in X_list:
        with rio.open(f's3://gfw-data-lake/umd_tree_cover_loss/v1.8/raster/epsg-4326/10/40000/year/geotiff/{Y}_{X}.tif') as src:
            tcl_arr = src.read(1, window=from_bounds(
                bounds[0],
                bounds[1],
                bounds[2],
                bounds[3],
                src.transform
        ))

        tcl_arrs.append(tcl_arr)
        print(f'Extracted window for {Y}, {X}')

# concatenate if multiple windows
tcl_arr = concatenate_windows(tcl_arrs, X_list, Y_list)

# mask tree cover loss by treecover threshold
print('Masking by treecover threshold')
tcl_masked = np.multiply(tcd_arr_mask, tcl_arr)

# Compute zonal statistics for admin area
print('Calculating tree cover loss in admin area per year')
loss_by_year_ha = {}
for year in tqdm(range(1,21)):
    # copy thresholded tree cover loss array
    tcl_arr_year = tcl_masked.copy()
    # mask by current year
    tcl_arr_year[np.where( tcl_masked != year )] = 0
    tcl_arr_year[np.where( tcl_masked == year )] = 1
    # convert to ha using area mask
    tcl_arr_year_area = np.multiply(area_mask, tcl_arr_year) / 10000
    # compute zonal stats for admin area
    zstats = zonal_stats(
        os.path.join('tmp', f'{os.path.basename(shp_fp[:-4])}_dissolved.shp'),
        tcl_arr_year_area,
        stats='sum',
        affine=win_affine,
        all_touched=False,
        nodata=-999
    )
    # log
    annual_loss_df = pd.DataFrame(zstats)
    annual_loss_df['year']=year+2000
    annual_loss = annual_loss_df['sum'].sum()
    loss_by_year_ha[year + 2000] = annual_loss

print(loss_by_year_ha)

# load into DataFrame
loss_df = pd.DataFrame.from_dict(loss_by_year_ha, orient='index')
loss_df = loss_df.rename(columns={0:'area_ha'})
loss_df['threshold'] = f'{treecover_threshold}%'
for i in range(len(contextual_layers)):
    loss_df[f'intersection_{i+1}'] = contextual_layers[i]

# save to CSV
loss_df.to_csv(os.path.join(
    OUT_DIR,
    f'{os.path.basename(shp_fp[:-4])}__tree_cover_loss_ha__tcd{treecover_threshold}.csv'),
    index_label='year'
)
