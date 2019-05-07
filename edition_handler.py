import simplejson as json

import common
import edition_repository


def post_edition(event, context):
    """POST /datasets/:dataset-id/versions/:version-id/editions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]

    edition_id = edition_repository.create_edition(dataset_id, version_id, content)

    if edition_id:
        return common.response(200, edition_id)
    else:
        return common.response(400, "Error creating edition.")


def update_edition(event, context):
    """PUT /datasets/:dataset-id/versions/:version-id/editions/:edition-id"""

    content = json.loads(event["body"])

    edition_id = event["pathParameters"]["edition-id"]

    if edition_repository.update_edition(edition_id, content):
        return common.response(200, edition_id)
    else:
        return common.response(
            404, "Selected edition does not exist. Could not update edition."
        )


def get_editions(event, context):
    """GET /datasets/:dataset-id/versions/:version-id/editions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]

    editions = edition_repository.get_editions(dataset_id, version_id)

    return common.response(200, editions)


def get_edition(event, context):
    """GET /datasets/:dataset-id/versions/:version-id/editions/:edition-id"""

    edition_id = event["pathParameters"]["edition-id"]

    edition = edition_repository.get_edition(edition_id)

    if edition:
        return common.response(200, edition)
    else:
        return common.response(404, "Selected edition does not exist.")
