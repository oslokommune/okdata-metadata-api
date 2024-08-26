import os

import simplejson as json
from aws_xray_sdk.core import xray_recorder
from okdata.aws.logging import logging_wrapper, log_add, log_exception

from metadata.error import ResourceConflict, ResourceNotFoundError, ValidationError
from metadata.common import error_response, response, validate_input
from metadata.auth import check_auth
from metadata.distribution.repository import DistributionRepository
from metadata.validator import Validator

validator = Validator("distribution")
BASE_URL = os.environ.get("BASE_URL", "")


@logging_wrapper
@validate_input(validator)
@check_auth("okdata:dataset:write", use_whitelist=True)
@xray_recorder.capture("create_distribution")
def create_distribution(event, context):
    """POST /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    content = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    log_add(dataset_id=dataset_id, version=version, edition=edition)

    try:
        # FIXME Remove once 'distribution_type' is required
        if "distribution_type" not in content:
            content["distribution_type"] = "file"

        distribution_repository = DistributionRepository()
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
        return response(201, body, headers)
    except ValidationError as e:
        log_exception(e)
        return error_response(400, str(e))
    except ResourceConflict as d:
        return error_response(409, f"Resource Conflict: {d}")
    except Exception as e:
        log_exception(e)
        message = f"Error creating distribution. RequestId: {context.aws_request_id}"
        return response(500, {"message": message})


@logging_wrapper
@validate_input(validator)
@check_auth("okdata:dataset:write", use_whitelist=True)
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

    try:
        distribution_repository = DistributionRepository()
        distribution_repository.update_distribution(
            dataset_id, version, edition, distribution, content
        )
        body = distribution_repository.get_distribution(
            dataset_id, version, edition, distribution, consistent_read=True
        )
        add_self_url(body)
        return response(200, body)
    except ValidationError as e:
        log_exception(e)
        return error_response(400, str(e))
    except KeyError:
        message = "Distribution not found."
        return response(404, {"message": message})
    except ValueError as e:
        log_exception(e)
        message = f"Error updating distribution. RequestId: {context.aws_request_id}"
        return response(500, {"message": message})


@logging_wrapper
@xray_recorder.capture("get_distributions")
def get_distributions(event, context):
    """GET /datasets/:dataset-id/versions/:version/editions/:edition/distributions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version = event["pathParameters"]["version"]
    edition = event["pathParameters"]["edition"]
    log_add(dataset_id=dataset_id, version=version, edition=edition)

    distributions = DistributionRepository().get_distributions(
        dataset_id, version, edition
    )
    log_add(num_distributions=len(distributions))

    for distribution in distributions:
        add_self_url(distribution)

    return response(200, distributions)


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

    content = DistributionRepository().get_distribution(
        dataset_id, version, edition, distribution
    )
    if content:
        add_self_url(content)
        return response(200, content)
    else:
        message = "Distribution not found."
        return response(404, {"message": message})


@logging_wrapper
@check_auth("okdata:dataset:write", use_whitelist=True)
@xray_recorder.capture("delete_distribution")
def delete_distribution(event, context):
    """DELETE /datasets/:dataset-id/versions/:version/editions/:edition/distributions/:distribution"""

    params = event["pathParameters"]
    dataset_id = params["dataset-id"]
    version = params["version"]
    edition = params["edition"]
    distribution = params["distribution"]
    log_add(
        dataset_id=dataset_id,
        version=version,
        edition=edition,
        distribution=distribution,
    )

    try:
        DistributionRepository().delete_item(
            f"{dataset_id}/{version}/{edition}/{distribution}"
        )
        return response(200, None)
    except ResourceNotFoundError as e:
        return response(404, {"message": str(e)})
    except ValueError as e:
        log_exception(e)
        message = f"Error deleting distribution. RequestId: {context.aws_request_id}"
        return response(500, {"message": message})


def add_self_url(distribution):
    if "Id" in distribution:
        (dataset_id, version, edition, distribution_id) = distribution["Id"].split("/")
        self_url = f"{BASE_URL}/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/{distribution_id}"
        distribution["_links"] = {"self": {"href": self_url}}
