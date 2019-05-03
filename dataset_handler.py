import simplejson as json

import common
import dataset_repository


def post_dataset(event, context):
    """POST /datasets"""

    content = json.loads(event["body"])

    dataset_id = dataset_repository.create_dataset(content)

    if dataset_id:
        return common.response(200, dataset_id)
    else:
        return common.response(400, "Error creating dataset.")


def update_dataset(event, context):
    """PUT /datasets/:dataset-id"""

    content = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    if dataset_repository.update_dataset(dataset_id, content):
        return common.response(200, dataset_id)
    else:
        return common.response(404, "Selected dataset does not exist. Could not update dataset.")


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
        return common.response(404, "Selected dataset does not exist.")
