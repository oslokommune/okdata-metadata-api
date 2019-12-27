import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

from metadata.CommonRepository import CommonRepository
from aws_xray_sdk.core import patch

patch(["boto3"])

edition_fmt = "%Y%m%dT%H%M%S"


class EditionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Edition")

    def edition_exists(self, dataset_id, version, edition):
        result = self.get_edition(dataset_id, version, edition)
        return result is not None

    def get_edition(self, dataset_id, version, edition, consistent_read=False):
        edition_id = f"{dataset_id}/{version}/{edition}"
        return self.get_item(edition_id, consistent_read)

    def get_editions(self, dataset_id, version):
        version_id = f"{dataset_id}/{version}"
        return self.get_items(version_id)

    def create_edition(self, dataset_id, version, content):
        edition_ts = datetime.fromisoformat(content["edition"]).astimezone(timezone.utc)
        edition_id = f"{dataset_id}/{version}/{edition_ts.strftime(edition_fmt)}"
        version_id = f"{dataset_id}/{version}"

        result = self.create_item(edition_id, content, version_id, "Version")

        latest = content.copy()
        latest["latest"] = edition_id
        latest_id = f"{dataset_id}/{version}/latest"
        self.create_item(latest_id, latest, update_on_exists=True)

        return result

    def update_latest_edition(self, dataset_id, version, edition, content):
        current_edition_id = f"{dataset_id}/{version}/{edition}"
        latest = content.copy()
        latest["latest"] = current_edition_id
        latest_id = f"{dataset_id}/{version}/latest"
        self.update_item(latest_id, content)

    def is_latest_edition(self, dataset_id, version, edition):
        try:
            current_edition_id = f"{dataset_id}/{version}/{edition}"
            latest_edition = self.get_edition(dataset_id, version, "latest")
            if (
                latest_edition is not None
                and "Id" in latest_edition
                and latest_edition["Id"] == current_edition_id
            ):
                return True
        except ClientError:
            return False
        return False

    def update_edition(self, dataset_id, version, edition, content):
        edition_id = f"{dataset_id}/{version}/{edition}"
        result = self.update_item(edition_id, content)
        if self.is_latest_edition(dataset_id, version, edition):
            self.update_latest_edition(dataset_id, version, edition, content)
        return result
