import simplejson as json

import common
import distribution_repository


def post_distribution(event, context):
    """POST /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]
    edition_id = event["pathParameters"]["edition-id"]

    distribution_id = distribution_repository.create_distribution(
        dataset_id, version_id, edition_id, content
    )

    if distribution_id:
        return common.response(200, distribution_id)
    else:
        return common.response(400, "Error creating distribution.")


def update_distribution(event, context):
    """PUT /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions/:distribution-id"""

    content = json.loads(event["body"])
    distribution_id = event["pathParameters"]["distribution-id"]

    if distribution_repository.update_distribution(distribution_id, content):
        return common.response(200, distribution_id)
    else:
        return common.response(
            404, "Selected distribution does not exist. Could not update distribution."
        )


def get_distributions(event, context):
    """GET /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]
    edition_id = event["pathParameters"]["edition-id"]

    distributions = distribution_repository.get_distributions(
        dataset_id, version_id, edition_id
    )

    return common.response(200, distributions)


def get_distribution(event, context):
    """GET /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions/:distribution-id"""

    distribution_id = event["pathParameters"]["distribution-id"]

    distribution = distribution_repository.get_distribution(distribution_id)

    if distribution:
        return common.response(200, distribution)
    else:
        return common.response(404, "Selected distribution does not exist.")
