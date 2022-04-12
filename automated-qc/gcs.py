import boto3
from google.cloud import storage
from google.oauth2 import service_account

def get_secret():
    '''
        Function to generate gcs credentials json through aws cli profile
    '''
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

def list_gcs_assets(bucket, prefix, gcs_credentials_json):
    credentials = service_account.Credentials.from_service_account_file(gcs_credentials_json)
    storage_client = storage.Client(credentials=credentials)
    blobs = storage_client.list_blobs(
        bucket, prefix=prefix, delimiter='*.tif'
    )
    paths = []
    for blob in blobs:
        path = blob.name
        path = '/vsigs/{}/{}'.format(bucket, path)
        paths.append(path)
    return paths