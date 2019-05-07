# -*- coding: utf-8 -*-

import simplejson as json

import common
import version_repository


def post_version(event, context):
    """POST /datasets/:dataset-id/versions"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    version_id = version_repository.create_version(dataset_id, content)

    if version_id:
        return common.response(200, version_id)
    else:
        return common.response(400, "Error creating version.")


def update_version(event, context):
    """PUT /datasets/:dataset-id/versions/:version-id"""

    content = json.loads(event["body"])
    version_id = event["pathParameters"]["version-id"]

    if version_repository.update_version(version_id, content):
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
    """GET /datasets/:dataset-id/versions/:version-id"""

    version_id = event["pathParameters"]["version-id"]

    version = version_repository.get_version(version_id)

    if version:
        return common.response(200, version)
    else:
        return common.response(404, "Selected version does not exist.")
