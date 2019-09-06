import simplejson as json
from aws_xray_sdk.core import xray_recorder

from metadata import common
from metadata.CommonRepository import ResourceConflict
from metadata.version.repository import VersionRepository
from auth import SimpleAuth

version_repository = VersionRepository()


@xray_recorder.capture("create_version")
def create_version(event, context):
    """POST /datasets/:dataset-id/versions"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    if not SimpleAuth().is_owner(event, dataset_id):
        return common.response(403, "Forbidden")

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


@xray_recorder.capture("update_version")
def update_version(event, context):
    """PUT /datasets/:dataset-id/versions/:version"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    if not SimpleAuth().is_owner(event, dataset_id):
        return common.response(403, "Forbidden")

    try:
        version_id = version_repository.update_version(dataset_id, version, content)
        return common.response(200, version_id)
    except KeyError:
        return common.response(404, "Version not found.")
    except ValueError as e:
        return common.response(400, f"Error updating version: {e}")


@xray_recorder.capture("get_versions")
def get_versions(event, context):
    """GET /datasets/:dataset-id/versions"""

    dataset_id = event["pathParameters"]["dataset-id"]

    versions = version_repository.get_versions(dataset_id)
    for version in versions:
        add_self_url(version)

    return common.response(200, versions)


@xray_recorder.capture("get_version")
def get_version(event, context):
    """GET /datasets/:dataset-id/versions/:version"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    content = version_repository.get_version(dataset_id, version)
    if content:
        add_self_url(content)
        return common.response(200, content)
    else:
        return common.response(404, "Version not found.")


def add_self_url(version):
    if "Id" in version:
        (dataset_id, version_name) = version["Id"].split("/")
        self_url = f"/datasets/{dataset_id}/versions/{version_name}"
        version["_links"] = {"self": {"href": self_url}}
