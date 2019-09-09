import simplejson as json
from aws_xray_sdk.core import xray_recorder

from metadata import common
from metadata.CommonRepository import ResourceConflict
from metadata.distribution.repository import DistributionRepository
from auth import SimpleAuth

distribution_repository = DistributionRepository()


@xray_recorder.capture("create_distribution")
def create_distribution(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    if not SimpleAuth().is_owner(event, dataset_id):
        return common.response(403, "Forbidden")

    try:
        distribution_id = distribution_repository.create_distribution(
            dataset_id, version, edition, content
        )

        distribution = distribution_id.split("/")[-1]
        location = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/{distribution}"
        headers = {"Location": location}

        return common.response(200, distribution_id, headers)
    except ResourceConflict as d:
        return common.response(409, f"Resource Conflict: {d}")
    except Exception as e:
        return common.response(400, f"Error creating distribution: {e}")


@xray_recorder.capture("update_distribution")
def update_distribution(event, context):
    """PUT /datasets/:dataset-id/versions/:version/editions/:edition/distributions/:distribution"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    distribution = event["pathParameters"]["distribution"]

    if not SimpleAuth().is_owner(event, dataset_id):
        return common.response(403, "Forbidden")

    try:
        distribution_id = distribution_repository.update_distribution(
            dataset_id, version, edition, distribution, content
        )

        return common.response(200, distribution_id)
    except KeyError:
        return common.response(404, "Distribution not found.")
    except ValueError as e:
        return common.response(400, f"Error updating distribution: {e}")


@xray_recorder.capture("get_distributions")
def get_distributions(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]

    distributions = distribution_repository.get_distributions(
        dataset_id, version, edition
    )

    for distribution in distributions:
        add_self_url(distribution)

    return common.response(200, distributions)


@xray_recorder.capture("get_distribution")
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
        add_self_url(content)
        return common.response(200, content)
    else:
        return common.response(404, "Distribution not found.")


def add_self_url(distribution):
    if "Id" in distribution:
        (dataset_id, version, edition, distribution_id) = distribution["Id"].split("/")
        self_url = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/{distribution_id}"
        distribution["_links"] = {"self": {"href": self_url}}
