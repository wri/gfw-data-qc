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
parser.add_argument('--admin_area', type=str, help='path to admin area shapefile')
parser.add_argument('--treecover_threshold', type=int, help='percent threshold for tree cover loss')
parser.add_argument('--intersections', type=str, help='contextual layers to intersect with admin area')
parser.add_argument('--output_directory', type=str, help='path to output directory for tree_cover_loss_ha.csv and dissolved_intersection.shp')
args = parser.parse_args()
shp_fp = args.admin_area
treecover_threshold = args.treecover_threshold
contextual_layers = args.intersections.split(' ')
OUT_DIR = args.output_directory

# create directories
if not os.path.exists(OUT_DIR):
    os.mkdir(OUT_DIR)

if not os.path.exists('tmp'):
    os.mkdir('tmp')

# get admin area bounds
with fiona.open(shp_fp) as src:
    bounds = src.bounds
X_list, Y_list = parse_bounds(bounds)

def intersect_layers(layers, bounds, shp_fp):
   
    # return dissolved shapefile if there are no contextual layers
    if len(contextual_layers) == 0:
        adm_shp = gpd.read_file(shp_fp)
        dissolved = gpd.GeoSeries(adm_shp.geometry).unary_union
        dissolved_gs = gpd.GeoSeries(dissolved)
        dissolved_gs.to_file(os.path.join('tmp', 'dissolved_intersection.shp'))
        
        return dissolved
    # parse for s3 paths
    s3_paths = [get_s3_asset_uri(layer) for layer in layers]
    # read contextual layers within bounds of admin area
    contextual_gdfs = []
    for s3_path in s3_paths:
        gdf = gpd.read_file(
            filename=f'zip+{s3_path}',
            bbox=bounds
        )
        contextual_gdfs.append(gdf)
    # intersect all layers
    print('Intersecting contextual layer(s) and admin area')
    intersected_gdf = gpd.read_file(shp_fp)
    for contextual_gdf in contextual_gdfs:
        try:
            intersected_gdf = gpd.overlay(intersected_gdf, contextual_gdf, how='intersection')
        except:
            continue
    # dissolve and save to tmp
    print('Dissolving intersected features')
    dissolved = gpd.GeoSeries(intersected_gdf.geometry).unary_union
    dissolved_gs = gpd.GeoSeries(dissolved)
    dissolved_gs.to_file(os.path.join('tmp', 'dissolved_intersection.shp'))

    return dissolved

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
        with rio.open(f's3://gfw-data-lake/umd_tree_cover_loss/v1.7/raster/epsg-4326/10/40000/year/geotiff/{Y}_{X}.tif') as src:
            tcl_arr = src.read(1, window=from_bounds(
                bounds[0],
                bounds[1],
                bounds[2],
                bounds[3],
                src.transform
        ))

        tcl_arrs.append(tcl_arr)

# concatenate if multiple windows
tcl_arr = concatenate_windows(tcl_arrs, X_list, Y_list)

# mask tree cover loss by treecover threshold
print('Masking by treecover threshold')
tcl_masked = np.multiply(tcd_arr_mask, tcl_arr)

# Compute zonal statistics for admin area
print('Calculating tree cover loss in admin area per year')
loss_by_year_ha = {}
for year in tqdm(range(1,20)):
    # copy thresholded tree cover loss array
    tcl_arr_year = tcl_masked.copy()
    # mask by current year
    tcl_arr_year[np.where( tcl_masked != year )] = 0
    tcl_arr_year[np.where( tcl_masked == year )] = 1
    # convert to ha using area mask
    tcl_arr_year_area = np.multiply(area_mask, tcl_arr_year) / 10000
    # compute zonal stats for admin area
    zstats = zonal_stats(
        os.path.join('tmp', 'dissolved_intersection.shp'),
        tcl_arr_year_area,
        stats='sum',
        affine=win_affine,
        all_touched=False,
        nodata=-999
    )
    # log
    annual_loss = zstats[0]['sum']
    loss_by_year_ha[year + 2000] = annual_loss

# load into DataFrame
loss_df = pd.DataFrame.from_dict(loss_by_year_ha, orient='index')
loss_df = loss_df.rename(columns={0:'area_ha'})
loss_df['threshold'] = f'{treecover_threshold}%'
print(loss_df)

# save to CSV
loss_df.to_csv(os.path.join(
    OUT_DIR,
    f'{os.path.basename(shp_fp[:-4])}_tree_cover_loss_ha.csv'),
    index_label='year'
)
print(f'Saved to {OUT_DIR}/{os.path.basename(shp_fp[:-4])}_tree_cover_loss_ha.csv')