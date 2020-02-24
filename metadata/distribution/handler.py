import simplejson as json
from aws_xray_sdk.core import xray_recorder

from auth import SimpleAuth
from dataplatform.awslambda.logging import logging_wrapper, log_add, log_exception
from metadata import common
from metadata.error import ResourceConflict
from metadata.common import validate_input
from metadata.distribution.repository import DistributionRepository
from metadata.validator import Validator

distribution_repository = DistributionRepository()
validator = Validator("distribution")


@logging_wrapper
@validate_input(validator)
@xray_recorder.capture("create_distribution")
def create_distribution(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    log_add(dataset_id=dataset_id, version=version, edition=edition)

    if not SimpleAuth().is_owner(event, dataset_id):
        return common.error_response(403, "Forbidden")

    try:
        distribution_id = distribution_repository.create_distribution(
            dataset_id, version, edition, content
        )

        distribution = distribution_id.split("/")[-1]
        log_add(distribution=distribution)

        location = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/{distribution}"
        headers = {"Location": location}
        body = distribution_repository.get_distribution(
            dataset_id, version, edition, distribution, consistent_read=True
        )
        add_self_url(body)
        return common.response(201, body, headers)
    except ResourceConflict as d:
        return common.error_response(409, f"Resource Conflict: {d}")
    except Exception as e:
        log_exception(e)
        message = f"Error creating distribution. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message},)


@logging_wrapper
@validate_input(validator)
@xray_recorder.capture("update_distribution")
def update_distribution(event, context):
    """PUT /datasets/:dataset-id/versions/:version/editions/:edition/distributions/:distribution"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    distribution = event["pathParameters"]["distribution"]
    log_add(
        dataset_id=dataset_id,
        version=version,
        edition=edition,
        distribution=distribution,
    )

    if not SimpleAuth().is_owner(event, dataset_id):
        return common.error_response(403, "Forbidden")

    try:
        distribution_repository.update_distribution(
            dataset_id, version, edition, distribution, content
        )
        body = distribution_repository.get_distribution(
            dataset_id, version, edition, distribution, consistent_read=True
        )
        add_self_url(body)
        return common.response(200, body)
    except KeyError:
        return common.error_response(404, "Distribution not found.")
    except ValueError as e:
        log_exception(e)
        message = f"Error updating distribution. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message},)


@logging_wrapper
@xray_recorder.capture("get_distributions")
def get_distributions(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    log_add(dataset_id=dataset_id, version=version, edition=edition)

    distributions = distribution_repository.get_distributions(
        dataset_id, version, edition
    )
    log_add(num_distributions=len(distributions))

    for distribution in distributions:
        add_self_url(distribution)

    return common.response(200, distributions)


@logging_wrapper
@xray_recorder.capture("get_distribution")
def get_distribution(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition/distributions/:distribution"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    distribution = event["pathParameters"]["distribution"]
    log_add(
        dataset_id=dataset_id,
        version=version,
        edition=edition,
        distribution=distribution,
    )

    content = distribution_repository.get_distribution(
        dataset_id, version, edition, distribution
    )
    if content:
        add_self_url(content)
        return common.response(200, content)
    else:
        return common.error_response(404, "Distribution not found.")


def add_self_url(distribution):
    if "Id" in distribution:
        (dataset_id, version, edition, distribution_id) = distribution["Id"].split("/")
        self_url = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/{distribution_id}"
        distribution["_links"] = {"self": {"href": self_url}}
