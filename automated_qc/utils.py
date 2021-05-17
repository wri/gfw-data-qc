import os
import math
import numpy as np
import geopandas as gpd
import requests
import rasterio.plot
import matplotlib.pyplot as plt
#from google.cloud import storage
#from google.oauth2 import service_account

# helper functions for tree cover loss qc

def parse_bounds(bounds):
    # parse upper left bounds to pull S3 tiles
    x1 = math.floor(bounds[0] / 10) * 10
    y1 = math.ceil(bounds[1] / 10) * 10
    x2 = math.floor(bounds[2] / 10) * 10
    y2 = math.ceil(bounds[3] / 10) * 10
    # check if bounds cover multiple S3 tiles
    if x1 != x2:
        Xs = [x1, x2]
    else:
        Xs = [x1]
    if y1 != y2:
        Ys = [y1, y2]
    else:
        Ys = [y1]
    # convert to string
    X_list, Y_list = [], []
    for X in Xs:
        if X > -10:
            X = "{:03d}E".format(X)
        else:
            X = "{:03d}W".format(X * -1)
        X_list.append(X)
    for Y in Ys:
        if Y > -10:
            Y = "{:02d}N".format(Y)
        else:
            Y = "{:02d}S".format(Y * -1)
        Y_list.append(Y)

    return X_list, Y_list

def concatenate_windows(win_arrs, X_list, Y_list):
    # if there are 4 tiles, concatenate on both axes
    if (len(X_list) > 1) and (len(Y_list) > 1):
        win_arr = np.concatenate(
                (
                    np.concatenate((win_arrs[2], win_arrs[3]), axis=1),
                    np.concatenate((win_arrs[0], win_arrs[1]), axis=1)
                ),
            axis=0
        )
    # otherwise, concatenate on one axis
    elif (len(X_list) > 1) and (len(Y_list) == 1):
        win_arr = np.concatenate((win_arrs[0], win_arrs[1]), axis=1)
    elif (len(X_list) == 1) and (len(Y_list) > 1):
        win_arr = np.concatenate((win_arrs[1], win_arrs[0]), axis=0)
    else:
        win_arr = win_arrs[0]

    return win_arr

def get_s3_asset_uri(dataset):
    if dataset == 'wdpa_protected_areas':
        res = requests.get(f'https://data-api.globalforestwatch.org/dataset/{dataset}/latest/assets?asset_type=Geopackage')
    else:
        res = requests.get(f'https://data-api.globalforestwatch.org/dataset/{dataset}/latest/assets?asset_type=ESRI Shapefile')
    return res.json()['data'][0]['asset_uri']

def overlay_feature_on_arr(feature, arr, affine, cmap):
    fig, ax = plt.subplots(figsize=(10,10))
    rasterio.plot.show(arr, ax=ax, transform=affine, cmap=cmap)
    feature.plot(ax=ax, facecolor='none', edgecolor='r')
    plt.axis('off')

def get_secret():
    secret_name = "gcs/gfw-gee-export"
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    secret = get_secret_value_response['SecretString']

    return secret

def intersect_layers(bounds, shp_fp, layers=None, dissolved=False):

    # return dissolved shapefile if there are no contextual layers
    if layers is None:
        if dissolved == True:
            adm_shp = gpd.read_file(shp_fp)
            dissolved = gpd.GeoSeries(adm_shp.geometry).unary_union
            dissolved_gs = gpd.GeoSeries(dissolved)
            dissolved_gs.to_file(os.path.join('tmp', 'dissolved_intersection.shp'))
            return dissolved
        else:
            return gpd.read_file(shp_fp)

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
        raise NoIntersectException('Admin area does not intersect with contextual layers')

    # intersect all layers
    intersected_gdf = gpd.read_file(shp_fp)
    for contextual_gdf in contextual_gdfs:
        try:
            intersected_gdf = gpd.overlay(intersected_gdf, contextual_gdf, how='intersection')
        except:
            continue

    # dissolve and save to tmp
    dissolved = gpd.GeoSeries(intersected_gdf.geometry).unary_union
    dissolved_gs = gpd.GeoSeries(dissolved)

    return dissolved_gs

class NoIntersectException(Exception):
    pass
