import os

import simplejson as json
from aws_xray_sdk.core import xray_recorder
from okdata.aws.logging import logging_wrapper, log_add, log_exception

from metadata.auth import check_auth
from metadata.common import error_response, response, validate_input
from metadata.error import DeleteConflict, ResourceConflict, ResourceNotFoundError
from metadata.error import InvalidVersionError
from metadata.validator import Validator
from metadata.version.repository import VersionRepository

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
        version_repository = VersionRepository()
        version_id = version_repository.create_version(dataset_id, content)

        version = version_id.split("/")[-1]
        log_add(version=version)

        location = f"/datasets/{dataset_id}/versions/{version}"
        headers = {"Location": location}
        body = version_repository.get_version(dataset_id, version, consistent_read=True)
        add_self_url(body)
        return response(201, body, headers)
    except ResourceConflict as d:
        return error_response(409, f"Resource Conflict: {d}")
    except Exception as e:
        log_exception(e)
        message = f"Error creating version. RequestId: {context.aws_request_id}"
        return response(500, {"message": message})


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
        version_repository = VersionRepository()
        version_repository.update_version(dataset_id, version, content)
        body = version_repository.get_version(dataset_id, version, consistent_read=True)
        add_self_url(body)
        return response(200, body)
    except KeyError:
        message = "Version not found."
        return response(404, {"message": message})
    except InvalidVersionError as e:
        return error_response(409, f"Invalid version data: {e}")
    except ValueError as e:
        log_exception(e)
        message = f"Error updating version. RequestId: {context.aws_request_id}"
        return response(500, {"message": message})


@logging_wrapper
@xray_recorder.capture("get_versions")
def get_versions(event, context):
    """GET /datasets/:dataset-id/versions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    log_add(dataset_id=dataset_id)

    versions = VersionRepository().get_versions(dataset_id)
    log_add(num_versions=len(versions))
    for version in versions:
        add_self_url(version)

    return response(200, versions)


@logging_wrapper
@xray_recorder.capture("get_version")
def get_version(event, context):
    """GET /datasets/:dataset-id/versions/:version"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    log_add(dataset_id=dataset_id, version=version)

    content = VersionRepository().get_version(dataset_id, version)
    if content:
        add_self_url(content)
        return response(200, content)
    else:
        message = "Version not found."
        return response(404, {"message": message})


@logging_wrapper
@check_auth("okdata:dataset:write", use_whitelist=True)
@xray_recorder.capture("delete_version")
def delete_version(event, context):
    """DELETE /datasets/:dataset-id/versions/:version"""

    params = event["pathParameters"]
    query_params = event.get("queryStringParameters") or {}
    dataset_id = params["dataset-id"]
    version = params["version"]
    log_add(dataset_id=dataset_id, version=version)

    try:
        VersionRepository().delete_item(
            f"{dataset_id}/{version}", query_params.get("cascade") == "true"
        )
        return response(200, None)
    except DeleteConflict as e:
        return response(400, {"message": str(e)})
    except ResourceNotFoundError as e:
        return response(404, {"message": str(e)})
    except ValueError as e:
        log_exception(e)
        return response(
            500,
            {"message": f"Error deleting version. RequestId: {context.aws_request_id}"},
        )


def add_self_url(version):
    if "Id" in version:
        (dataset_id, version_name) = version["Id"].split("/")
        self_url = f"{BASE_URL}/datasets/{dataset_id}/versions/{version_name}"
        version["_links"] = {"self": {"href": self_url}}
