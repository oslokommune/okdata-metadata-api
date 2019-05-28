import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone

import common
from CommonRepository import CommonRepository


edition_fmt = "%Y%m%dT%H%M%S"


class EditionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        self.metadata_table = dynamodb.Table("dataset-metadata")
        self.edition_table = dynamodb.Table(common.table_name_prefix + "-edition")

        super().__init__(
            self.metadata_table, "Edition", self.edition_table, common.EDITION_ID
        )

    def edition_exists(self, dataset_id, version, edition):
        result = self.get_edition(dataset_id, version, edition)
        return result is not None

    def get_edition(self, dataset_id, version, edition):
        edition_id = f"{dataset_id}/{version}/{edition}"
        return self.get_item(edition_id, edition)

    def get_editions(self, dataset_id, version):
        version_id = f"{dataset_id}/{version}"
        legacy_filter = Key(common.DATASET_ID).eq(dataset_id) & Key(
            common.VERSION_ID
        ).eq(version)
        return self.get_items(version_id, legacy_filter)

    def create_edition(self, dataset_id, version, content):
        edition_ts = datetime.fromisoformat(content["edition"]).astimezone(timezone.utc)
        edition_id = f"{dataset_id}/{version}/{edition_ts.strftime(edition_fmt)}"
        version_id = f"{dataset_id}/{version}"

        return self.create_item(edition_id, content, version_id, "Version")

    def update_edition(self, dataset_id, version, edition, content):
        edition_id = f"{dataset_id}/{version}/{edition}"
        return self.update_item(edition_id, content)
