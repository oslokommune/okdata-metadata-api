import simplejson as json

import common
from dataset_repository import DatasetRepository


dataset_repository = DatasetRepository()


def create_dataset(event, context):
    """POST /datasets"""

    content = json.loads(event["body"])

    try:
        dataset_id = dataset_repository.create_dataset(content)
        return common.response(200, dataset_id)
    except Exception as e:
        return common.response(400, f"Error creating dataset: {e}")


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


def get_datasets(event, context):
    """GET /datasets"""

    datasets = dataset_repository.get_datasets()

    return common.response(200, datasets)


def get_dataset(event, context):
    """GET /datasets/:dataset-id"""

    dataset_id = event["pathParameters"]["dataset-id"]
    dataset = dataset_repository.get_dataset(dataset_id)

    if dataset:
        return common.response(200, dataset)
    else:
        return common.response(404, "Dataset not found.")
