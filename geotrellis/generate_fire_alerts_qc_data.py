import geopandas as gpd
from shapely.geometry import Point
import boto3

BUCKET = "gfw-data-lake"

admin2 = gpd.read_file("/Users/justin.terry/dev/data/qc_areas.shp")
s3 = boto3.client("s3")
response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix ='nasa_modis_fire_alerts/v6/vector/epsg-4326/tsv/near_real_time/')

nrts = []
for result in response['Contents'][1:]:
    nrt = gpd.read_file(f"s3://{BUCKET}/{result['Key']}")
    nrts.append(nrt)

scientific_2020 = gpd.read_file("s3://gfw-data-lake/nasa_modis_fire_alerts/v6/vector/epsg-4326/tsv/scientific/MODIS_2020.tsv")
alerts = gpd.pd.concat(nrts + [scientific_2020])

alerts['latitude'] = gpd.pd.to_numeric(alerts['latitude'])
alerts['longitude'] = gpd.pd.to_numeric(alerts['longitude'])
alerts['geometry'] = alerts.apply(lambda row: Point(row.longitude, row.latitude), axis=1)
alerts = alerts.set_crs(epsg=4326)

joined = gpd.sjoin(alerts, admin2, op="intersects")
print(joined.size)
joined.drop(['geometry', 'index_right', 'iso', 'id_1', 'id_2'], axis=1).to_csv('/Users/justin.terry/dev/gfw_forest_loss_geotrellis/input/fires_modis_qc.tsv', sep='\t', index=False)