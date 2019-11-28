import os
import simplejson as json
from aws_xray_sdk.core import xray_recorder

from auth import SimpleAuth
from dataplatform.awslambda.logging import logging_wrapper, log_add
from metadata import common
from metadata.error import ResourceConflict
from metadata.common import validate_input
from metadata.dataset.repository import DatasetRepository
from metadata.validator import Validator

dataset_repository = DatasetRepository()
AUTHORIZER_API = os.environ["AUTHORIZER_API"]
validator = Validator("dataset")


@logging_wrapper("metadata-api")
@validate_input(validator)
@xray_recorder.capture("create_dataset")
def create_dataset(event, context):
    """POST /datasets"""

    content = json.loads(event["body"])

    try:
        dataset_id = dataset_repository.create_dataset(content)
        log_add(dataset_id=dataset_id)

        user_id = event["requestContext"]["authorizer"]["principalId"]

        requests = SimpleAuth().request_from_client()
        requests.post(f"{AUTHORIZER_API}/{dataset_id}", json={"principalId": user_id})

        headers = {"Location": f"/datasets/{dataset_id}"}
        body = dataset_repository.get_dataset(dataset_id)
        add_self_url(body)
        return common.response(201, body, headers)
    except ResourceConflict as d:
        return common.error_response(409, f"Resource Conflict: {d}")
    except Exception as e:
        return common.error_response(400, f"Error creating dataset: {e}")


@logging_wrapper("metadata-api")
@validate_input(validator)
@xray_recorder.capture("update_dataset")
def update_dataset(event, context):
    """PUT /datasets/:dataset-id"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    log_add(dataset_id=dataset_id)

    if not SimpleAuth().is_owner(event, dataset_id):
        return common.error_response(403, "Forbidden")

    try:
        dataset_repository.update_dataset(dataset_id, content)
        body = dataset_repository.get_dataset(dataset_id)
        add_self_url(body)
        return common.response(200, body)
    except KeyError:
        return common.error_response(404, "Dataset not found.")
    except ValueError as e:
        return common.error_response(400, f"Error updating dataset: {e}")


@logging_wrapper("metadata-api")
@xray_recorder.capture("get_datasets")
def get_datasets(event, context):
    """GET /datasets"""

    datasets = dataset_repository.get_datasets()
    log_add(num_datasets=len(datasets))

    for dataset in datasets:
        add_self_url(dataset)

    return common.response(200, datasets)


@logging_wrapper("metadata-api")
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
        return common.error_response(404, "Dataset not found.")


def add_self_url(dataset):
    if "Id" in dataset:
        self_url = f'/datasets/{dataset["Id"]}'
        dataset["_links"] = {"self": {"href": self_url}}
