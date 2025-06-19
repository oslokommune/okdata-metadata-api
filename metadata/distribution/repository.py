import logging
import mimetypes
import os
import uuid

import boto3
from aws_xray_sdk.core import patch

from metadata.common import BOTO_RESOURCE_COMMON_KWARGS, CONFIDENTIALITY_MAP, STAGES
from metadata.CommonRepository import CommonRepository
from metadata.error import ResourceNotFoundError, ValidationError
from metadata.util import getenv

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

patch(["boto3"])

API_NAMESPACES = ["okdata-api-catalog"]

# Parquet doesn't have an IANA-registered MIME type
# (yet? https://issues.apache.org/jira/browse/PARQUET-1889).
mimetypes.add_type("application/parquet", ".parq")
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
        api_id = content.get("api_id")

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
            if api_id:
                namespace, ref = api_id.split(":")
                if namespace not in API_NAMESPACES:
                    raise ValidationError(
                        f"API namespace must be one of {API_NAMESPACES}, was '{namespace}'."
                    )
        elif api_url or api_id:
            raise ValidationError(
                "'{}' is only supported when 'distribution_type' is 'api', got '{}'.".format(
                    "api_url" if api_url else "api_id", distribution_type
                )
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

        mime_type, encoding = mimetypes.guess_type(filename)

        if mime_type:
            item["content_type"] = mime_type

    def _delete_data(self, dataset_id, version, edition, distribution):
        """Delete data from S3 belonging to the given distribution."""
        from metadata.dataset.repository import DatasetRepository

        distribution_id = f"{dataset_id}/{version}/{edition}/{distribution}"
        distribution_ = self.get_item(distribution_id)

        if not distribution_:
            raise ResourceNotFoundError

        bucket = getenv("DATA_BUCKET_NAME")
        dataset = DatasetRepository().get_dataset(dataset_id)

        if not dataset:
            logger.warning(f"Unknown dataset '{dataset_id}'; skipping data deletion")
            return

        access_rights = dataset.get("accessRights")
        confidentiality = CONFIDENTIALITY_MAP.get(access_rights)
        filenames = distribution_.get("filenames")

        if not confidentiality:
            logger.warning(
                f"Unknown confidentiality for dataset '{dataset_id}'; skipping data deletion"
            )
            return

        if not filenames:
            logger.warning(
                f"No filenames listed for distribution '{distribution_id}'; skipping data deletion"
            )
            return

        s3 = boto3.client("s3", region_name=getenv("AWS_REGION"))

        for stage in STAGES:
            for filename in filenames:
                prefix = f"{stage}/{confidentiality}/{dataset_id}/version={version}/edition={edition}/{filename}"
                logger.debug(f"Looking for {prefix}")
                objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

                if not objects or "Contents" not in objects:
                    logger.debug(f"No data to delete for stage '{stage}'")
                    continue

                s3_keys = [c["Key"] for c in objects["Contents"]]
                logger.debug(f"To delete: {s3_keys}")
                response = s3.delete_objects(
                    Bucket=bucket, Delete={"Objects": [{"Key": k for k in s3_keys}]}
                )
                logger.debug(f"Deleted: {response.get('Deleted')}")

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

    def delete_item(self, item_id, cascade=False):
        dataset_id, version, edition, distribution = item_id.split("/")
        self._delete_data(dataset_id, version, edition, distribution)
        return super().delete_item(item_id, cascade)

    def children(self, item_id):
        return []

    def child_repository(self):
        return None
