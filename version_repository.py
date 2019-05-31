import boto3
from boto3.dynamodb.conditions import Key

import common
from CommonRepository import CommonRepository


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

    def update_version(self, dataset_id, version, content):
        version_id = f"{dataset_id}/{version}"
        return self.update_item(version_id, content)
