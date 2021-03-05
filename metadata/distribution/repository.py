import mimetypes
import uuid

import boto3
from aws_xray_sdk.core import patch

from metadata.CommonRepository import CommonRepository
from metadata.common import BOTO_RESOURCE_COMMON_KWARGS
from metadata.error import ValidationError

patch(["boto3"])


def _ensure_extra_mime_types():
    # Parquet doesn't have an IANA-registered MIME type
    # (yet? https://issues.apache.org/jira/browse/PARQUET-1889).
    if ".parq" not in mimetypes.types_map:
        mimetypes.add_type("application/parquet", ".parq")
    if ".parquet" not in mimetypes.types_map:
        mimetypes.add_type("application/parquet", ".parquet")


class DistributionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", **BOTO_RESOURCE_COMMON_KWARGS)

        self.metadata_table = dynamodb.Table("dataset-metadata")

        super().__init__(self.metadata_table, "Distribution")

    def _validate_content(self, content):
        distribution_type = content.get("distribution_type")
        filename = content.get("filename")
        filenames = content.get("filenames")
        api_url = content.get("api_url")

        if distribution_type == "file":
            if not (filename or filenames):
                raise ValidationError(
                    # Not exactly true – we still accept the deprecated
                    # 'filename' – but let's recommend the undeprecated one.
                    "Missing 'filenames', required when 'distribution_type' is 'file'."
                )
        elif filename or filenames:
            raise ValidationError(
                "'filename{}' is only supported when 'distribution_type' is 'file', got '{}'.".format(
                    "s" if filenames else "",
                    distribution_type,
                )
            )

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

    @staticmethod
    def _derive_content_type(item):
        """Make an attempt at deriving a content type for `item`."""

        if "content_type" in item or item.get("distribution_type") != "file":
            return

        filename = (
            item["filenames"][0] if item.get("filenames") else item.get("filename")
        )

        if not filename:
            return

        _ensure_extra_mime_types()
        mime_type, encoding = mimetypes.guess_type(filename)

        if mime_type:
            item["content_type"] = mime_type

    def get_distribution(
        self, dataset_id, version, edition, distribution, consistent_read=False
    ):
        distribution_id = f"{dataset_id}/{version}/{edition}/{distribution}"
        item = self.get_item(distribution_id, consistent_read)

        if item:
            self._derive_content_type(item)

        return item

    def get_distributions(self, dataset_id, version, edition):
        edition_id = f"{dataset_id}/{version}/{edition}"
        items = self.get_items(edition_id)

        for item in items:
            self._derive_content_type(item)

        return items

    def create_distribution(self, dataset_id, version, edition, content):
        self._validate_content(content)

        distribution_id = f"{dataset_id}/{version}/{edition}/{uuid.uuid4()}"
        edition_id = f"{dataset_id}/{version}/{edition}"

        return self.create_item(distribution_id, content, edition_id, "Edition")

    def update_distribution(self, dataset_id, version, edition, distribution, content):
        self._validate_content(content)

        distribution_id = f"{dataset_id}/{version}/{edition}/{distribution}"
        return self.update_item(distribution_id, content)
