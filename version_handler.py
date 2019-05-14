# -*- coding: utf-8 -*-

import simplejson as json

import common
from version_repository import VersionRepository

version_repository = VersionRepository()


def create_version(event, context):
    """POST /datasets/:dataset-id/versions"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    version_id = version_repository.create_version(dataset_id, content)

    if version_id:
        return common.response(200, version_id)
    else:
        return common.response(400, "Error creating version.")


def update_version(event, context):
    """PUT /datasets/:dataset-id/versions/:version"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    if version_repository.update_version(dataset_id, version, content):
        # TODO move to repository response
        version_id = f"{dataset_id}#{version}"

        return common.response(200, version_id)
    else:
        return common.response(
            404, "Selected version does not exist. Could not update version."
        )


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
        return common.response(404, "Selected version does not exist.")
