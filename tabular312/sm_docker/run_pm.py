import os
import time
import mimetypes
import json
import shutil
import pickle
import traceback
import argparse
import warnings
import boto3
from botocore.exceptions import ClientError
import papermill as pm
from papermill.exceptions import PapermillExecutionError
import pprint

import run_pm_utils as utils
import conf

pp = pprint.PrettyPrinter(width=41, compact=True, indent=4)
warnings.filterwarnings('ignore')


def upload_file_to_s3(local_path: str, bucket: str, prefix: str) -> str:
    """
    Upload a local file to S3 under s3://{bucket}/{prefix}/<basename>.
    Returns the S3 URI.
    """
    if not os.path.isfile(local_path):
        raise FileNotFoundError(f"Local file not found: {local_path}")

    s3 = boto3.client("s3")

    filename = os.path.basename(local_path)
    prefix = prefix.strip("/")
    key = f"{prefix}/{filename}" if prefix else filename

    content_type, _ = mimetypes.guess_type(local_path)
    extra_args = {}
    if content_type:
        extra_args["ContentType"] = content_type

    try:
        s3.upload_file(local_path, bucket, key, ExtraArgs=extra_args or None)
    except ClientError as e:
        raise RuntimeError(f"S3 upload failed: {e}") from e

    return f"s3://{bucket}/{key}"
    

def run_papermill(input_nb, output_dir):
    os.chdir(output_dir)
    output_nb = input_nb.replace('.ipynb', '_output.ipynb')
    try:
        pm.execute_notebook(
            input_nb,
            output_nb,
            parameters=dict(),
            kernel_name=conf.kernel_name,
            report_mode=True
        )
        bucket_name = "retail-mlops-edu-202602"
        s3_prefix = "output"
        s3_uri = upload_file_to_s3(output_nb, bucket_name, s3_prefix)
        print(f"Uploaded to: {s3_uri}")
    except PapermillExecutionError as e:
        pp.pprint(e)
        pass


# ----------------------------
# Main Execution
# ----------------------------
if __name__ == "__main__":
    try:
        output_dir = "."
        artifacts_dir = "artifacts"
        os.makedirs(artifacts_dir, exist_ok=True)
        input_nb = 'titanic-competition-step-by-step-using-xgboost.ipynb'
        run_papermill(input_nb, output_dir)
    except Exception as e:
        print(e)