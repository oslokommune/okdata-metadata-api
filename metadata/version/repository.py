import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from metadata import common
from metadata.CommonRepository import CommonRepository
from aws_xray_sdk.core import patch

patch(["boto3"])


class VersionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        self.metadata_table = dynamodb.Table("dataset-metadata")
        self.version_table = dynamodb.Table(common.table_name_prefix + "-version")

        super().__init__(
            self.metadata_table, "Version", self.version_table, common.VERSION_ID
        )

    def version_exists(self, dataset_id, version):
        result = self.get_version(dataset_id, version)
        return result is not None

    def get_version(self, dataset_id, version):
        version_id = f"{dataset_id}/{version}"
        return self.get_item(version_id, version)

    def get_versions(self, dataset_id):
        legacy_filter = Key(common.DATASET_ID).eq(dataset_id)
        return self.get_items(dataset_id, legacy_filter)

    def create_version(self, dataset_id, content):
        version = content["version"]
        version_id = f"{dataset_id}/{version}"

        result = self.create_item(version_id, content, dataset_id, "Dataset")

        latest = content.copy()
        latest["latest"] = version_id
        latest_id = f"{dataset_id}/latest"
        self.create_item(latest_id, latest, update_on_exists=True)

        return result

    def update_latest_version(self, dataset_id, version_id, content):
        latest = content.copy()
        latest["latest"] = version_id
        latest_id = f"{dataset_id}/latest"
        result = False
        try:
            result = self.update_item(latest_id, content)
        except ClientError:
            # TODO: should we create a /latest version if it doesn't exist?
            result = True

        return result

    def update_version(self, dataset_id, version, content):
        version_id = f"{dataset_id}/{version}"
        update_result = self.update_item(version_id, content)

        update_latest_result = self.update_latest_version(dataset_id, version, content)
        if update_result and update_latest_result:
            return version_id

        return False
