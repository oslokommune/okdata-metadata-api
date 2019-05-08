import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import distribution_handler
import common_test_helper


class DistributionTest(unittest.TestCase):
    @mock_dynamodb2
    def test_post_distribution(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        dataset_table = common_test_helper.create_dataset_table(dynamodb)
        version_table = common_test_helper.create_version_table(dynamodb)
        edition_table = common_test_helper.create_edition_table(dynamodb)
        common_test_helper.create_distribution_table(dynamodb)

        dataset_table.put_item(Item=common_test_helper.dataset)
        version_table.put_item(Item=common_test_helper.version)
        edition_table.put_item(Item=common_test_helper.edition)

        create_event = {
            "body": json.dumps(common_test_helper.new_distribution),
            "pathParameters": {
                "dataset-id": common_test_helper.distribution[table.DATASET_ID],
                "version-id": common_test_helper.distribution[table.VERSION_ID],
                "edition-id": common_test_helper.distribution[table.EDITION_ID],
            },
        }

        response = distribution_handler.post_distribution(create_event, None)
        assert response["statusCode"] == 200

        bad_create_event = create_event
        bad_create_event["pathParameters"]["edition-id"] = "LOLOLFEIL"

        response = distribution_handler.post_distribution(bad_create_event, None)
        assert response["statusCode"] == 400

    @mock_dynamodb2
    def test_update_distribution(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        distribution_table = common_test_helper.create_distribution_table(dynamodb)

        distribution = common_test_helper.distribution
        distribution_id = distribution[table.DISTRIBUTION_ID]
        distribution_table.put_item(Item=distribution)

        update_event = {
            "body": json.dumps(common_test_helper.distribution_updated),
            "pathParameters": {
                "dataset-id": distribution[table.DATASET_ID],
                "version-id": distribution[table.VERSION_ID],
                "edition-id": distribution[table.EDITION_ID],
                "distribution-id": distribution_id,
            },
        }

        response = distribution_handler.update_distribution(update_event, None)
        assert json.loads(response["body"]) == distribution_id

        db_response = distribution_table.query(
            KeyConditionExpression=Key(table.DISTRIBUTION_ID).eq(distribution_id)
        )
        assert db_response["Items"][0]["filename"] == "UPDATED.csv"

    @mock_dynamodb2
    def test_get_all_distributions(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        distribution_table = common_test_helper.create_distribution_table(dynamodb)
        distribution_table.put_item(Item=common_test_helper.distribution)
        distribution_table.put_item(Item=common_test_helper.distribution_updated)

        get_all_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.distribution[table.DATASET_ID],
                "version-id": common_test_helper.distribution[table.VERSION_ID],
                "edition-id": common_test_helper.distribution[table.EDITION_ID],
            }
        }

        response = distribution_handler.get_distributions(get_all_event, None)
        distributions_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(distributions_from_db) == 2

    @mock_dynamodb2
    def test_should_fetch_distribution_from_new_table_if_present(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        distribution_table = common_test_helper.create_distribution_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        distribution_table.put_item(Item=common_test_helper.distribution)
        metadata_table.put_item(Item=common_test_helper.distribution_new_format)

        distribution_id = common_test_helper.distribution_new_format[table.ID_COLUMN]
        get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.dataset[table.DATASET_ID],
                "version-id": common_test_helper.version["version"],
                "edition-id": common_test_helper.edition["edition"],
                "distribution-id": common_test_helper.distribution["filename"],
            }
        }

        response = distribution_handler.get_distribution(get_event, None)
        distribution_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert distribution_from_db[table.ID_COLUMN] == distribution_id

    @mock_dynamodb2
    def test_get_one_distribution_legacy(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        distribution_table = common_test_helper.create_distribution_table(dynamodb)
        distribution_table.put_item(Item=common_test_helper.distribution)
        distribution_table.put_item(Item=common_test_helper.distribution_updated)

        distribution_id = common_test_helper.distribution[table.DISTRIBUTION_ID]
        get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.distribution[table.DATASET_ID],
                "version-id": common_test_helper.distribution[table.VERSION_ID],
                "edition-id": common_test_helper.distribution[table.EDITION_ID],
                "distribution-id": distribution_id,
            }
        }

        response = distribution_handler.get_distribution(get_event, None)
        distribution_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert distribution_from_db[table.DISTRIBUTION_ID] == distribution_id

    @mock_dynamodb2
    def test_distribution_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_distribution_table(dynamodb)

        event_for_get = {
            "pathParameters": {
                "dataset-id": "1234",
                "version-id": "1",
                "edition-id": "20190401T133700",
                "distribution-id": "file.txt",
            }
        }

        response = distribution_handler.get_distribution(event_for_get, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
