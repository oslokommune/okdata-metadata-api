import boto3
from boto3.dynamodb.conditions import Key

import common
from CommonRepository import CommonRepository


class DistributionRepository(CommonRepository):
    def __init__(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        self.metadata_table = dynamodb.Table("dataset-metadata")
        self.distribution_table = dynamodb.Table(
            common.table_name_prefix + "-distribution"
        )

        super().__init__(
            self.metadata_table,
            "Distribution",
            self.distribution_table,
            common.DISTRIBUTION_ID,
        )

    def distribution_exists(self, dataset_id, version, edition, distribution):
        result = self.get_distribution(dataset_id, version, edition, distribution)
        return result is not None

    def get_distribution(self, dataset_id, version, edition, distribution):
        distribution_id = f"{dataset_id}#{version}#{edition}#{distribution}"
        return self.get_item(distribution_id, distribution)

    def get_distributions(self, dataset_id, version, edition):
        edition_id = f"{dataset_id}#{version}#{edition}"
        legacy_filter = (
            Key(common.DATASET_ID).eq(dataset_id)
            & Key(common.VERSION_ID).eq(version)
            & Key(common.EDITION_ID).eq(edition)
        )
        return self.get_items(edition_id, legacy_filter)

    def create_distribution(self, dataset_id, version, edition, content):
        filename = content["filename"]
        distribution_id = f"{dataset_id}#{version}#{edition}#{filename}"
        edition_id = f"{dataset_id}#{version}#{edition}"

        return self.create_item(distribution_id, content, edition_id, "Edition")

    def update_distribution(self, dataset_id, version, edition, distribution, content):
        distribution_id = f"{dataset_id}#{version}#{edition}#{distribution}"
        return self.update_item(distribution_id, content)
