import simplejson as json

import common
import distribution_repository


def create_distribution(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    distribution_id = distribution_repository.create_distribution(
        dataset_id, version, edition, content
    )

    if distribution_id:
        return common.response(200, distribution_id)
    else:
        return common.response(400, "Error creating distribution.")


def update_distribution(event, context):
    """PUT /datasets/:dataset-id/versions/:version/editions/:edition/distributions/:distribution"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    distribution = event["pathParameters"]["distribution"]

    if distribution_repository.update_distribution(
        dataset_id, version, edition, distribution, content
    ):
        return common.response(200, distribution)
    else:
        return common.response(
            404, "Selected distribution does not exist. Could not update distribution."
        )


def get_distributions(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    distributions = distribution_repository.get_distributions(
        dataset_id, version, edition
    )

    return common.response(200, distributions)


def get_distribution(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition/distributions/:distribution"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    distribution = event["pathParameters"]["distribution"]

    content = distribution_repository.get_distribution(
        dataset_id, version, edition, distribution
    )

    if content:
        return common.response(200, content)
    else:
        return common.response(404, "Selected distribution does not exist.")
