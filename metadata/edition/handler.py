import os

import simplejson as json
from aws_xray_sdk.core import xray_recorder
from okdata.aws.logging import logging_wrapper, log_add, log_exception

from metadata import common
from metadata.error import ResourceConflict
from metadata.auth import check_auth
from metadata.common import validate_input
from metadata.edition.repository import EditionRepository
from metadata.validator import Validator


edition_repository = EditionRepository()
validator = Validator("edition")
BASE_URL = os.environ.get("BASE_URL", "")


@logging_wrapper
@validate_input(validator)
@check_auth("okdata:dataset:write")
@xray_recorder.capture("create_edition")
def create_edition(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    log_add(dataset_id=dataset_id, version=version)

    try:
        edition_id = edition_repository.create_edition(dataset_id, version, content)

        edition = edition_id.split("/")[-1]
        log_add(edition=edition)

        location = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}"
        headers = {"Location": location}
        body = edition_repository.get_edition(
            dataset_id, version, edition, consistent_read=True
        )
        add_self_url(body)
        return common.response(201, body, headers)
    except ResourceConflict as d:
        return common.error_response(409, f"Resource Conflict: {d}")
    except KeyError as ke:
        return common.error_response(404, str(ke))
    except Exception as e:
        log_exception(e)
        message = f"Error creating edition. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message})


@logging_wrapper
@validate_input(validator)
@check_auth("okdata:dataset:write")
@xray_recorder.capture("update_edition")
def update_edition(event, context):
    """PUT /datasets/:dataset-id/versions/:version/editions/:edition"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    log_add(dataset_id=dataset_id, version=version, edition=edition)

    try:
        edition_repository.update_edition(dataset_id, version, edition, content)
        body = edition_repository.get_edition(
            dataset_id, version, edition, consistent_read=True
        )
        add_self_url(body)
        return common.response(200, body)
    except KeyError as ke:
        return common.error_response(404, str(ke))
    except ValueError as e:
        log_exception(e)
        message = f"Error updating edition. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message})


@logging_wrapper
@xray_recorder.capture("get_editions")
def get_editions(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    log_add(dataset_id=dataset_id, version=version)

    editions = edition_repository.get_editions(dataset_id, version)
    log_add(num_editions=len(editions))
    for edition in editions:
        add_self_url(edition)

    return common.response(200, editions)


@logging_wrapper
@xray_recorder.capture("get_edition")
def get_edition(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    log_add(dataset_id=dataset_id, version=version, edition=edition)

    content = edition_repository.get_edition(dataset_id, version, edition)
    if content:
        add_self_url(content)
        return common.response(200, content)
    else:
        message = "Edition not found."
        return common.response(404, {"message": message})


def add_self_url(edition):
    if "Id" in edition:
        (dataset_id, version, edition_name) = edition["Id"].split("/")
        self_url = f"{BASE_URL}/datasets/{dataset_id}/versions/{version}/editions/{edition_name}"
        edition["_links"] = {"self": {"href": self_url}}
