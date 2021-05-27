import os

import requests
import simplejson as json
from aws_xray_sdk.core import xray_recorder

from okdata.aws.logging import logging_wrapper, log_add, log_exception
from metadata import common
from metadata.auth import Auth, check_auth
from metadata.error import ResourceConflict, ValidationError
from metadata.common import validate_input
from metadata.dataset.repository import DatasetRepository
from metadata.validator import Validator

dataset_repository = DatasetRepository()
OKDATA_PERMISSION_API_URL = os.environ["OKDATA_PERMISSION_API_URL"]
validator = Validator("dataset")
patch_validator = Validator("dataset_patch")
BASE_URL = os.environ.get("BASE_URL", "")


@logging_wrapper
@validate_input(validator)
@xray_recorder.capture("create_dataset")
def create_dataset(event, context):
    """POST /datasets"""
    content = json.loads(event["body"], use_decimal=True)

    try:
        dataset_id = dataset_repository.create_dataset(content)
        log_add(dataset_id=dataset_id)

        principal_id = event["requestContext"]["authorizer"]["principalId"]

        auth_header = Auth().service_client_authorization_header()

        create_okdata_permissions(
            dataset_id=dataset_id,
            owner_principal_id=principal_id,
            auth_header=auth_header,
        )

        headers = {"Location": f"/datasets/{dataset_id}"}
        body = dataset_repository.get_dataset(dataset_id, consistent_read=True)
        add_self_url(body)
        return common.response(201, body, headers)
    except ValidationError as e:
        log_exception(e)
        return common.error_response(400, str(e))
    except ResourceConflict as d:
        return common.error_response(409, f"Resource Conflict: {d}")
    except Exception as e:
        log_exception(e)
        message = f"Error creating dataset. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message})


@logging_wrapper
@validate_input(validator)
@check_auth("okdata:dataset:update")
@xray_recorder.capture("update_dataset")
def update_dataset(event, context):
    """PUT /datasets/:dataset-id"""

    return _update_dataset(event, context, patch=False)


@logging_wrapper
@validate_input(patch_validator)
@check_auth("okdata:dataset:update")
@xray_recorder.capture("patch_dataset")
def patch_dataset(event, context):
    """PATCH /datasets/:dataset-id"""

    return _update_dataset(event, context, patch=True)


@logging_wrapper
@xray_recorder.capture("get_datasets")
def get_datasets(event, context):
    """GET /datasets"""

    query_params = (
        event["queryStringParameters"] if event["queryStringParameters"] else {}
    )

    datasets = dataset_repository.get_datasets(
        parent_id=query_params.get("parent_id"),
        api_id=query_params.get("api_id"),
    )
    log_add(num_datasets=len(datasets))

    for dataset in datasets:
        add_self_url(dataset)

    return common.response(200, datasets)


@logging_wrapper
@xray_recorder.capture("get_dataset")
def get_dataset(event, context):
    """GET /datasets/:dataset-id"""

    dataset_id = event["pathParameters"]["dataset-id"]
    log_add(dataset_id=dataset_id)
    dataset = dataset_repository.get_dataset(dataset_id)

    if dataset:
        add_self_url(dataset)
        return common.response(200, dataset)
    else:
        message = "Dataset not found."
        return common.response(404, {"message": message})


def add_self_url(dataset):
    if "Id" in dataset:
        self_url = f'{BASE_URL}/datasets/{dataset["Id"]}'
        dataset["_links"] = {"self": {"href": self_url}}


def _update_dataset(event, context, patch):
    content = json.loads(event["body"], use_decimal=True)
    dataset_id = event["pathParameters"]["dataset-id"]
    log_add(dataset_id=dataset_id)

    try:
        if patch:
            dataset_repository.patch_dataset(dataset_id, content)
        else:
            dataset_repository.update_dataset(dataset_id, content)
        body = dataset_repository.get_dataset(dataset_id, consistent_read=True)
        add_self_url(body)
        return common.response(200, body)
    except KeyError:
        message = "Dataset not found"
        return common.response(404, {"message": message})
    except ValidationError as e:
        log_exception(e)
        return common.response(400, {"message": str(e)})
    except ValueError as e:
        log_exception(e)
        message = f"Error updating dataset. RequestId: {context.aws_request_id}"
        return common.response(500, {"message": message})


def create_okdata_permissions(
    dataset_id: str, owner_principal_id: str, auth_header: dict
):
    resource_name = f"okdata:dataset:{dataset_id}"
    service_account_prefix = "service-account-"
    if owner_principal_id.startswith(service_account_prefix):
        user_id = owner_principal_id[len(service_account_prefix) :]
        user_type = "client"
    else:
        user_type = "user"
        user_id = owner_principal_id
    create_permissions_body = {
        "owner": {"user_id": user_id, "user_type": user_type},
        "resource_name": resource_name,
    }
    create_permissions_response = requests.post(
        f"{OKDATA_PERMISSION_API_URL}/permissions",
        json=create_permissions_body,
        headers=auth_header,
    )
    create_permissions_response.raise_for_status()
