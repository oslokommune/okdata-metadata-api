import re

import boto3
import shortuuid
from aws_xray_sdk.core import patch
from botocore.exceptions import ClientError
from difflib import SequenceMatcher
from okdata.aws.logging import log_dynamodb, log_exception

from metadata.CommonRepository import CommonRepository, TYPE_COLUMN, ID_COLUMN
from metadata.common import BOTO_RESOURCE_COMMON_KWARGS
from metadata.error import ValidationError

patch(["boto3"])


class DatasetRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", **BOTO_RESOURCE_COMMON_KWARGS)

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
        """Create a new dataset with `content` and return its ID.

        Verify that a parent dataset exists with source type `none` when a
        `parent_id` is given, otherwise raise a `ValidationError`.
        """

        parent_id = content.get("parent_id")
        if parent_id:
            parent = self.get_dataset(parent_id)

            if not parent:
                raise ValidationError(f"Parent dataset '{parent_id}' doesn't exist.")

            source_type = parent.get("source").get("type")
            if source_type != "none":
                raise ValidationError(
                    f"Wrong parent source type. Got '{source_type}', expected 'none'."
                )

        title = content["title"]
        dataset_id = self.generate_unique_id_based_on_title(title)

        content["state"] = "active"

        content[ID_COLUMN] = dataset_id
        content[TYPE_COLUMN] = self.type

        if "source" not in content:
            content["source"] = {"type": "file"}

        version = {
            "version": "1",
            ID_COLUMN: f"{dataset_id}/1",
            TYPE_COLUMN: "Version",
        }

        latest = version.copy()
        latest["latest"] = version[ID_COLUMN]
        latest[ID_COLUMN] = f"{dataset_id}/latest"

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
