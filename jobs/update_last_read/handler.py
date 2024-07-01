import logging
import os

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import logging_wrapper

from jobs.update_last_read.util import getenv, yesterday

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

patch_all()


@logging_wrapper
@xray_recorder.capture("handler")
def handler(event, context):
    """TODO"""

    s3 = boto3.resource("s3")
    data_bucket_name = getenv("DATA_BUCKET_NAME")
    logs_bucket_name = getenv("LOGS_BUCKET_NAME")
    timestamp = yesterday()
    prefix = f"logs/s3/{data_bucket_name}/{timestamp}"

    for obj in s3.Bucket(logs_bucket_name).objects.filter(Prefix=prefix):
        print(obj)
        # matches = body_to_matches(obj.get()["Body"].read().decode(), pat)
        # print(matches)
