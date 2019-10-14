import boto3
from botocore.exceptions import ClientError

from metadata.CommonRepository import CommonRepository
from aws_xray_sdk.core import patch

patch(["boto3"])


class VersionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Version")

    def version_exists(self, dataset_id, version):
        result = self.get_version(dataset_id, version)
        return result is not None

    def get_version(self, dataset_id, version):
        version_id = f"{dataset_id}/{version}"
        return self.get_item(version_id)

    def get_versions(self, dataset_id):
        return self.get_items(dataset_id)

    def create_version(self, dataset_id, content):
        version = content["version"]
        version_id = f"{dataset_id}/{version}"

        result = self.create_item(version_id, content, dataset_id, "Dataset")

        latest = content.copy()
        latest["latest"] = version_id
        latest_id = f"{dataset_id}/latest"
        self.create_item(latest_id, latest, update_on_exists=True)

        return result

    def update_latest_version(self, dataset_id, version, content):
        current_version_id = f"{dataset_id}/{version}"
        latest = content.copy()
        latest["latest"] = current_version_id
        latest_id = f"{dataset_id}/latest"
        self.update_item(latest_id, content)

    def is_latest_version(self, dataset_id, version):
        try:
            current_version_id = f"{dataset_id}/{version}"
            latest_version = self.get_version(dataset_id, "latest")
            if (
                latest_version is not None
                and "Id" in latest_version
                and latest_version["Id"] == current_version_id
            ):
                return True
        except ClientError:
            return False
        return False

    def update_version(self, dataset_id, version, content):
        version_id = f"{dataset_id}/{version}"
        result = self.update_item(version_id, content)
        if self.is_latest_version(dataset_id, version):
            self.update_latest_version(dataset_id, version, content)
        return result