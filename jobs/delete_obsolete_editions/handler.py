import logging
import os

from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import logging_wrapper

from jobs.delete_obsolete_editions.editions import obsolete_editions

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

patch_all()


@logging_wrapper
@xray_recorder.capture("handler")
def handler(event, context):
    for ed in obsolete_editions():
        # TODO: Find all distributions belonging to these editions. Delete
        # distribution and edition metadata, plus any relevant S3 data.
        pass
