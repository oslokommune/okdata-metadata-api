import simplejson as json

import common
from CommonRepository import ResourceConflict
from edition_repository import EditionRepository

edition_repository = EditionRepository()
from aws_xray_sdk.core import xray_recorder


@xray_recorder.capture('create_edition')
def create_edition(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    try:
        edition_id = edition_repository.create_edition(dataset_id, version, content)

        edition = edition_id.split("/")[-1]
        location = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}"
        headers = {"Location": location}

        return common.response(200, edition_id, headers)
    except ResourceConflict as d:
        return common.response(409, f"Resource Conflict: {d}")
    except Exception as e:
        return common.response(400, f"Error creating edition: {e}")

@xray_recorder.capture('update_edition')
def update_edition(event, context):
    """PUT /datasets/:dataset-id/versions/:version/editions/:edition"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    try:
        edition_id = edition_repository.update_edition(
            dataset_id, version, edition, content
        )
        return common.response(200, edition_id)
    except KeyError:
        return common.response(404, "Edition not found.")
    except ValueError as e:
        return common.response(400, f"Error updating edition: {e}")

@xray_recorder.capture('get_editions')
def get_editions(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    editions = edition_repository.get_editions(dataset_id, version)
    for edition in editions:
        add_self_url(edition)

    return common.response(200, editions)

@xray_recorder.capture('get_edition')
def get_edition(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    content = edition_repository.get_edition(dataset_id, version, edition)
    if content:
        add_self_url(content)
        return common.response(200, content)
    else:
        return common.response(404, "Edition not found.")


def add_self_url(edition):
    if "Id" in edition:
        (dataset_id, version, edition_name) = edition["Id"].split("/")
        self_url = f"/datasets/{dataset_id}/versions/{version}/editions/{edition_name}"
        edition["_links"] = {"self": {"href": self_url}}
