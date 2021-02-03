import uuid

import boto3
from aws_xray_sdk.core import patch

from metadata.CommonRepository import CommonRepository
from metadata.common import BOTO_RESOURCE_COMMON_KWARGS

patch(["boto3"])


class DistributionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", **BOTO_RESOURCE_COMMON_KWARGS)

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Distribution")

    def distribution_exists(self, dataset_id, version, edition, distribution):
        result = self.get_distribution(dataset_id, version, edition, distribution)
        return result is not None

    def get_distribution(
        self, dataset_id, version, edition, distribution, consistent_read=False
    ):
        distribution_id = f"{dataset_id}/{version}/{edition}/{distribution}"
        return self.get_item(distribution_id, consistent_read)

    def get_distributions(self, dataset_id, version, edition):
        edition_id = f"{dataset_id}/{version}/{edition}"
        return self.get_items(edition_id)

    def create_distribution(self, dataset_id, version, edition, content):
        distribution_id = f"{dataset_id}/{version}/{edition}/{uuid.uuid4()}"
        edition_id = f"{dataset_id}/{version}/{edition}"

        return self.create_item(distribution_id, content, edition_id, "Edition")

    def update_distribution(self, dataset_id, version, edition, distribution, content):
        distribution_id = f"{dataset_id}/{version}/{edition}/{distribution}"
        return self.update_item(distribution_id, content)
