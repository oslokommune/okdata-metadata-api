import uuid

import boto3
from aws_xray_sdk.core import patch

from metadata.CommonRepository import CommonRepository
from metadata.common import BOTO_RESOURCE_COMMON_KWARGS
from metadata.error import ValidationError

patch(["boto3"])


class DistributionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", **BOTO_RESOURCE_COMMON_KWARGS)

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Distribution")

    def _validate_content(self, content):
        distribution_type = content.get("distribution_type")
        api_url = content.get("api_url")

        if distribution_type == "api":
            if not api_url:
                raise ValidationError(
                    "Missing 'api_url', required when 'distribution_type' is 'api'."
                )
        elif api_url:
            raise ValidationError(
                f"'api_url' is only supported when 'distribution_type' is 'api', got '{distribution_type}'."
            )

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
        self._validate_content(content)

        distribution_id = f"{dataset_id}/{version}/{edition}/{uuid.uuid4()}"
        edition_id = f"{dataset_id}/{version}/{edition}"

        return self.create_item(distribution_id, content, edition_id, "Edition")

    def update_distribution(self, dataset_id, version, edition, distribution, content):
        self._validate_content(content)

        distribution_id = f"{dataset_id}/{version}/{edition}/{distribution}"
        return self.update_item(distribution_id, content)
