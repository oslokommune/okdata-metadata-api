import simplejson as json

import common
from CommonRepository import ResourceConflict
from dataset_repository import DatasetRepository
from aws_xray_sdk.core import xray_recorder


dataset_repository = DatasetRepository()

@xray_recorder.capture('create_dataset')
def create_dataset(event, context):
    """POST /datasets"""

    content = json.loads(event["body"])

    try:
        dataset_id = dataset_repository.create_dataset(content)

        headers = {"Location": f"/datasets/{dataset_id}"}

        return common.response(200, dataset_id, headers)
    except ResourceConflict as d:
        return common.response(409, f"Resource Conflict: {d}")
    except Exception as e:
        return common.response(400, f"Error creating dataset: {e}")

@xray_recorder.capture('update_dataset')
def update_dataset(event, context):
    """PUT /datasets/:dataset-id"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    try:
        dataset_repository.update_dataset(dataset_id, content)
        return common.response(200, dataset_id)
    except KeyError:
        return common.response(404, "Dataset not found.")
    except ValueError as e:
        return common.response(400, f"Error updating dataset: {e}")

@xray_recorder.capture('get_datasets')
def get_datasets(event, context):
    """GET /datasets"""

    datasets = dataset_repository.get_datasets()
    for dataset in datasets:
        add_self_url(dataset)

    return common.response(200, datasets)

@xray_recorder.capture('get_dataset')
def get_dataset(event, context):
    """GET /datasets/:dataset-id"""

    dataset_id = event["pathParameters"]["dataset-id"]
    dataset = dataset_repository.get_dataset(dataset_id)

    if dataset:
        add_self_url(dataset)
        return common.response(200, dataset)
    else:
        return common.response(404, "Dataset not found.")


def add_self_url(dataset):
    if "Id" in dataset:
        self_url = f'/datasets/{dataset["Id"]}'
        dataset["_links"] = {"self": {"href": self_url}}
