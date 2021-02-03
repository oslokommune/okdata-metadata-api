import boto3
import re
import shortuuid

from botocore.exceptions import ClientError
from difflib import SequenceMatcher

from okdata.aws.logging import log_dynamodb, log_exception
from metadata import common
from metadata.CommonRepository import CommonRepository
from aws_xray_sdk.core import patch

patch(["boto3"])


class DatasetRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", **common.BOTO_RESOURCE_COMMON_KWARGS)

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Dataset")

    def dataset_exists(self, dataset_id):
        dataset = self.get_dataset(dataset_id)
        return dataset is not None

    def get_dataset(self, dataset_id, consistent_read=False):
        return self.get_item(dataset_id, consistent_read)

    def get_datasets(self, parent_id=None):
        return self.get_items(parent_id)

    def create_dataset(self, content):
        """Create a new dataset with `content` and return its ID."""
        title = content["title"]
        dataset_id = self.generate_unique_id_based_on_title(title)

        content[common.ID_COLUMN] = dataset_id
        content[common.TYPE_COLUMN] = self.type
        content["state"] = "active"

        version = {
            "version": "1",
            common.ID_COLUMN: f"{dataset_id}/1",
            common.TYPE_COLUMN: "Version",
        }

        latest = version.copy()
        latest["latest"] = version[common.ID_COLUMN]
        latest[common.ID_COLUMN] = f"{dataset_id}/latest"

        try:
            db_response = log_dynamodb(
                lambda: self.metadata_table.meta.client.transact_write_items(
                    TransactItems=[
                        {"Put": {"Item": item, "TableName": "dataset-metadata"}}
                        for item in [content, version, latest]
                    ]
                )
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            msg = e.response["Error"]["Message"]
            log_exception(msg)
            raise ValueError(f"Error creating dataset ({error_code}): {msg}")

        status_code = db_response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            return dataset_id
        else:
            msg = f"Error creating dataset ({status_code}): {db_response}"
            log_exception(msg)
            raise ValueError(msg)

    def update_dataset(self, dataset_id, content):
        return self.update_item(dataset_id, content)

    def patch_dataset(self, dataset_id, content):
        return self.patch_item(dataset_id, content)

    def generate_unique_id_based_on_title(self, title):
        id = slugify(title)[:64]
        if self.dataset_exists(id):
            return id + "-" + shortuuid.ShortUUID().random(length=5)
        else:
            return id


def slugify(title):
    a = "àáäâãåăçèéëêæǵḧìíïîḿńǹñòóöôœøṕŕßśșțùúüûǘẃẍÿź_"
    b = "aaaaaaaceeeeeghiiiimnnnooooooprssstuuuuuwxyz "
    tr = str.maketrans(a, b)
    t = re.sub("[^a-z0-9]+", "-", title.lower().translate(tr))
    if t[0] == "-":
        t = t[1:]
    if t[-1] == "-":
        t = t[0:-1]
    return t


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()
