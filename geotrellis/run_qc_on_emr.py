import os
import json
from datetime import datetime

import click
import boto3

ENV = os.environ.get("ENV", "staging")
ENV_PREFIX = "" if ENV == 'production' else f"-{ENV}"
PIPELINES_BUCKET = f"gfw-data-lake{ENV_PREFIX}"
CURR_DATETIME = datetime.now().strftime("%Y-%m-%d_%H-%M")

QC_AREAS_PATH = "s3://gfw-pipelines/geotrellis/features/qc/gadm_36_1_1__qc_areas.tsv"
MODIS_QC_PATH = "s3://gfw-pipelines/geotrellis/features/qc/modis_alerts__qc.tsv"
VIIRS_QC_PATH = "s3://gfw-pipelines/geotrellis/features/qc/viirs_alerts__qc.tsv"

SUBMIT_EMR_JOB_LAMBDA = "datapump-submit_job-default"


@click.command()
@click.option("--geotrellis_jar_path", default=None)
@click.option("--output_path", default=f"s3://{PIPELINES_BUCKET}/geotrellis/results/qc/{CURR_DATETIME}")
def run_qc_on_emr(geotrellis_jar_path, output_path):
    payload = {
        "instance_count": 5,
        "feature_src": QC_AREAS_PATH,
        "feature_type": "geostore",
        "analyses": ["gladalerts", "annualupdate_minimal", "firealerts"],
        "name": f"geotrellis-qc-{CURR_DATETIME}",
        "get_summary": False,
        "fire_config": {
            "viirs": [VIIRS_QC_PATH],
            "modis": [MODIS_QC_PATH],
        },
        "geotrellis_jar": geotrellis_jar_path
    }

    session = boto3.session.Session(profile_name=f"gfw-{ENV}")
    lambda_client = session.client("lambda")
    response = lambda_client.invoke(
        FunctionName=SUBMIT_EMR_JOB_LAMBDA,
        InvocationType="RequestResponse",
        Payload=bytes(json.dumps(payload), "utf-8")
    )

    response_payload = json.loads(response['Payload'].read().decode())
    if response_payload["status"] == "SUCCESS":
        print("EMR job successfully started! See EMR dashboard for progress.")
        print(f"Job Flow ID: {response_payload['job_flow_id']}")
    elif response_payload["status"] == "FAILED":
        print("There was an issue starting EMR job. See logs on AWS for details.")


if __name__ == "__main__":
    run_qc_on_emr()