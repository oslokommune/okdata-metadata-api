import logging
import os
from datetime import datetime, timedelta

import boto3
from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import logging_wrapper

from jobs.update_last_read.dataset import DatasetEntry
from jobs.update_last_read.logrec import LogRecord
from jobs.update_last_read.util import getenv
from metadata.dataset.repository import DatasetRepository

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

patch_all()


def _yesterday():
    """Return yesterday's date on the format YYYY-MM-DD."""
    return (datetime.now() - timedelta(days=1)).isoformat()[:10]


@logging_wrapper
@xray_recorder.capture("handler")
def handler(event, context):
    s3 = boto3.resource("s3")
    data_bucket_name = getenv("DATA_BUCKET_NAME")
    logs_bucket_name = getenv("LOGS_BUCKET_NAME")
    timestamp = _yesterday()
    prefix = f"logs/s3/{data_bucket_name}/{timestamp}"

    datasets_read = {}
    dataset_repository = DatasetRepository()

    for obj in s3.Bucket(logs_bucket_name).objects.filter(Prefix=prefix):
        log_records = [
            log_record
            for log_record in map(
                LogRecord.from_log_line,
                obj.get()["Body"].read().decode().strip().split("\n"),
            )
            if log_record.operation == "REST.GET.OBJECT"
        ]

        for log_record in log_records:
            if dataset_entry := DatasetEntry.from_s3_key(log_record.key):
                dataset_id = dataset_entry.dataset_id
                dt = log_record.datetime()

                if dataset_id not in datasets_read or datasets_read[dataset_id] < dt:
                    datasets_read[dataset_id] = dt

    for dataset, dt in datasets_read.items():
        if dataset_repository.dataset_exists(dataset):
            dataset_repository.patch_dataset(dataset, {"last_read": dt.isoformat()})
