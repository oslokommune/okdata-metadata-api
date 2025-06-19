import logging
import os
from operator import itemgetter

from aws_xray_sdk.core import patch_all, xray_recorder
from okdata.aws.logging import log_add, logging_wrapper

from jobs.delete_obsolete_editions.editions import obsolete_editions
from metadata.edition.repository import EditionRepository

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

patch_all()


@logging_wrapper
@xray_recorder.capture("handler")
def handler(event, context):
    edition_repository = EditionRepository()
    num_deleted_editions = 0

    for edition_id in map(itemgetter("Id"), obsolete_editions()):
        logger.info(f"Deleting edition {edition_id}")
        edition_repository.delete_item(edition_id, cascade=True)
        num_deleted_editions += 1

    log_add(num_deleted_editions=num_deleted_editions)
