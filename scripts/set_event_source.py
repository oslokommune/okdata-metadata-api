import os
import boto3
from metadata.dataset.repository import DatasetRepository
from aws_xray_sdk.core import xray_recorder

xray_recorder.begin_segment("Set-event-source-script")

env = "dev"


class EventStreamsTable:
    def __init__(self):
        table_name = "event-streams"
        dynamodb = boto3.resource("dynamodb", region_name="eu-west-1")
        self.table = dynamodb.Table(table_name)

    def get_dataset_ids(self):
        items = self.table.scan()["Items"]
        return set([item["id"].split("/")[0] for item in items])


if __name__ == "__main__":
    if env == "dev":
        os.environ["AWS_PROFILE"] = "okdata-dev"
    elif env == "prod":
        os.environ["AWS_PROFILE"] = "okdata-prod"
    else:
        raise Exception(f"Invalid env {env}")
    dataset_ids = EventStreamsTable().get_dataset_ids()
    [print(dataset_id) for dataset_id in dataset_ids]

    dataset_repository = DatasetRepository()

    for dataset_id in dataset_ids:
        dataset_repository.patch_item(dataset_id, {"source": {"type": "event"}})
