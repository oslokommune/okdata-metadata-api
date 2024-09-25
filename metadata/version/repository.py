import boto3
from aws_xray_sdk.core import patch
from botocore.exceptions import ClientError

from metadata.common import BOTO_RESOURCE_COMMON_KWARGS
from metadata.CommonRepository import CommonRepository
from metadata.edition.repository import EditionRepository
from metadata.error import InvalidVersionError

patch(["boto3"])


class VersionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", **BOTO_RESOURCE_COMMON_KWARGS)

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Version")

    def version_exists(self, dataset_id, version):
        result = self.get_version(dataset_id, version)
        return result is not None

    def get_version(self, dataset_id, version, consistent_read=False):
        version_id = f"{dataset_id}/{version}"
        return self.get_item(version_id, consistent_read)

    def get_versions(self, dataset_id, exclude_latest=True):
        versions = self.get_items(dataset_id)

        if exclude_latest:
            # Remove 'latest' version/edition
            return list(filter(lambda i: "latest" not in i, versions))
        return versions

    def create_version(self, dataset_id, content):
        """Create a new version of `dataset_id` with `content` and return its ID.

        Also create (or update if it exists) a "latest" version pointing to the
        new version.
        """
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
        if content["version"] != version:
            content_version = content["version"]
            raise InvalidVersionError(
                f"Version {content_version} in body is not equal to {version} "
            )
        version_id = f"{dataset_id}/{version}"
        result = self.update_item(version_id, content)
        if self.is_latest_version(dataset_id, version):
            self.update_latest_version(dataset_id, version, content)
        return result

    def children(self, item_id):
        return self._query_children(item_id, "Edition")

    def child_repository(self):
        return EditionRepository()
