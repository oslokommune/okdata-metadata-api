import os

import simplejson as json
from aws_xray_sdk.core import xray_recorder
from okdata.aws.logging import logging_wrapper, log_add, log_exception

from metadata import common
from metadata.error import ResourceConflict
from metadata.auth import check_auth
from metadata.common import validate_input
from metadata.validator import Validator
from metadata.version.repository import VersionRepository
from metadata.error import InvalidVersionError

version_repository = VersionRepository()
validator = Validator("version")
BASE_URL = os.environ.get("BASE_URL", "")


@logging_wrapper
@validate_input(validator)
@check_auth("okdata:dataset:update")
@xray_recorder.capture("create_version")
def create_version(event, context):
    """POST /datasets/:dataset-id/versions"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    log_add(dataset_id=dataset_id)

    try:
        version_id = version_repository.create_version(dataset_id, content)

        version = version_id.split("/")[-1]
        log_add(version=version)

        location = f"/datasets/{dataset_id}/versions/{version}"
        headers = {"Location": location}
        body = version_repository.get_version(dataset_id, version, consistent_read=True)
        add_self_url(body)
        return common.response(201, body, headers)
    except ResourceConflict as d:
        return common.error_response(409, f"Resource Conflict: {d}")
    except Exception as e:
        log_exception(e)
        message = f"Error creating version. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message})


@logging_wrapper
@validate_input(validator)
@check_auth("okdata:dataset:update")
@xray_recorder.capture("update_version")
def update_version(event, context):
    """PUT /datasets/:dataset-id/versions/:version"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    log_add(dataset_id=dataset_id, version=version)

    try:
        version_repository.update_version(dataset_id, version, content)
        body = version_repository.get_version(dataset_id, version, consistent_read=True)
        add_self_url(body)
        return common.response(200, body)
    except KeyError:
        message = "Version not found."
        return common.response(404, {"message": message})
    except InvalidVersionError as e:
        return common.error_response(409, f"Invalid version data: {e}")
    except ValueError as e:
        log_exception(e)
        message = f"Error updating version. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message})


@logging_wrapper
@xray_recorder.capture("get_versions")
def get_versions(event, context):
    """GET /datasets/:dataset-id/versions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    log_add(dataset_id=dataset_id)

    versions = version_repository.get_versions(dataset_id)
    log_add(num_versions=len(versions))
    for version in versions:
        add_self_url(version)

    return common.response(200, versions)


@logging_wrapper
@xray_recorder.capture("get_version")
def get_version(event, context):
    """GET /datasets/:dataset-id/versions/:version"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    log_add(dataset_id=dataset_id, version=version)

    content = version_repository.get_version(dataset_id, version)
    if content:
        add_self_url(content)
        return common.response(200, content)
    else:
        message = "Version not found."
        return common.response(404, {"message": message})


def add_self_url(version):
    if "Id" in version:
        (dataset_id, version_name) = version["Id"].split("/")
        self_url = f"{BASE_URL}/datasets/{dataset_id}/versions/{version_name}"
        version["_links"] = {"self": {"href": self_url}}
