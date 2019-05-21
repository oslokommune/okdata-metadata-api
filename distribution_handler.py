import simplejson as json

import common
from distribution_repository import DistributionRepository

distribution_repository = DistributionRepository()


def create_distribution(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    try:
        distribution_id = distribution_repository.create_distribution(
            dataset_id, version, edition, content
        )

        distribution = distribution_id.split("/")[-1]
        location = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/{distribution}"
        headers = {"Location": location}

        return common.response(200, distribution_id, headers)
    except Exception as e:
        return common.response(400, f"Error creating distribution: {e}")


def update_distribution(event, context):
    """PUT /datasets/:dataset-id/versions/:version/editions/:edition/distributions/:distribution"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    distribution = event["pathParameters"]["distribution"]

    try:
        distribution_id = distribution_repository.update_distribution(
            dataset_id, version, edition, distribution, content
        )

        return common.response(200, distribution_id)
    except KeyError:
        return common.response(404, "Distribution not found.")
    except ValueError as e:
        return common.response(400, f"Error updating distribution: {e}")


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
        return common.response(404, "Distribution not found.")
