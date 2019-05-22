import simplejson as json

import common
from CommonRepository import ResourceConflict
from edition_repository import EditionRepository

edition_repository = EditionRepository()


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


def get_editions(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    editions = edition_repository.get_editions(dataset_id, version)

    return common.response(200, editions)


def get_edition(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    content = edition_repository.get_edition(dataset_id, version, edition)
    if content:
        return common.response(200, content)
    else:
        return common.response(404, "Edition not found.")
