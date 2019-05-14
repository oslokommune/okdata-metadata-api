import simplejson as json

import common
import edition_repository


def create_edition(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]

    edition_id = edition_repository.create_edition(dataset_id, version, content)

    if edition_id:
        return common.response(200, edition_id)
    else:
        return common.response(400, "Error creating edition.")


def update_edition(event, context):
    """PUT /datasets/:dataset-id/versions/:version/editions/:edition"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    if edition_repository.update_edition(dataset_id, version, edition, content):
        return common.response(200, edition)
    else:
        return common.response(
            404, "Selected edition does not exist. Could not update edition."
        )


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
        return common.response(404, "Selected edition does not exist.")
