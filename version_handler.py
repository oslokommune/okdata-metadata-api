# -*- coding: utf-8 -*-

import simplejson as json

import common
from CommonRepository import ResourceConflict
from version_repository import VersionRepository

version_repository = VersionRepository()


def create_version(event, context):
    """POST /datasets/:dataset-id/versions"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    try:
        version_id = version_repository.create_version(dataset_id, content)

        version = version_id.split("/")[-1]
        location = f"/datasets/{dataset_id}/versions/{version}"
        headers = {"Location": location}

        return common.response(200, version_id, headers)
    except ResourceConflict as d:
        return common.response(409, f"Resource Conflict: {d}")
    except Exception as e:
        return common.response(400, f"Error creating version: {e}")


def update_version(event, context):
    """PUT /datasets/:dataset-id/versions/:version"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    try:
        version_id = version_repository.update_version(dataset_id, version, content)
        return common.response(200, version_id)
    except KeyError:
        return common.response(404, "Version not found.")
    except ValueError as e:
        return common.response(400, f"Error updating version: {e}")


def get_versions(event, context):
    """GET /datasets/:dataset-id/versions"""

    dataset_id = event["pathParameters"]["dataset-id"]

    versions = version_repository.get_versions(dataset_id)

    return common.response(200, versions)


def get_version(event, context):
    """GET /datasets/:dataset-id/versions/:version"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    content = version_repository.get_version(dataset_id, version)
    if content:
        return common.response(200, content)
    else:
        return common.response(404, "Version not found.")
