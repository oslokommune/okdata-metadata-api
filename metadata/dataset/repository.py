import boto3
from boto3.dynamodb.conditions import Key
import re
import shortuuid

from difflib import SequenceMatcher

from metadata import common
from metadata.CommonRepository import CommonRepository
from aws_xray_sdk.core import patch

patch(["boto3"])


class DatasetRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Dataset")

    def dataset_exists(self, dataset_id):
        dataset = self.get_dataset(dataset_id)
        return dataset is not None

    def get_dataset(self, dataset_id):
        return self.get_item(dataset_id)

    def get_datasets(self):
        db_response = self.metadata_table.query(
            IndexName="IdByTypeIndex",
            KeyConditionExpression=Key(common.TYPE_COLUMN).eq("Dataset"),
        )
        return db_response["Items"]

    def create_dataset(self, content):
        title = content["title"]
        dataset_id = self.generate_unique_id_based_on_title(title)
        return self.create_item(dataset_id, content)

    def update_dataset(self, dataset_id, content):
        return self.update_item(dataset_id, content)

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