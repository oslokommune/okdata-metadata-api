import json
import re
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import distribution_handler
import common_test_helper


class DistributionTest(unittest.TestCase):
    @mock_dynamodb2
    def test_create_distribution(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_distribution_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]

        create_event = {
            "body": json.dumps(common_test_helper.new_distribution),
            "pathParameters": {
                "dataset-id": dataset_id,
                "version": common_test_helper.version["version"],
                "edition": "20190528T133700",
            },
        }
        response = distribution_handler.create_distribution(create_event, None)
        distribution_id = json.loads(response["body"])

        id_regex = f"{dataset_id}/6/20190528T133700/[0-9a-f-]+"
        location_regex = f"/datasets/{dataset_id}/versions/6/editions/20190528T133700/distributions/[0-9a-f-]+"

        assert response["statusCode"] == 200
        assert re.fullmatch(id_regex, distribution_id)
        assert re.fullmatch(location_regex, response["headers"]["Location"])

        # Creating distribution for non-existing edition should fail
        bad_create_event = create_event
        bad_create_event["pathParameters"]["edition"] = "LOLOLFEIL"

        response = distribution_handler.create_distribution(bad_create_event, None)
        assert response["statusCode"] == 400

    @mock_dynamodb2
    def test_update_distribution(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        metadata_table = common_test_helper.create_metadata_table(dynamodb)
        metadata_table.put_item(Item=common_test_helper.distribution_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        distribution_id = (
            f"{dataset_id}/6/20190528T133700/e80b5f2c-67f0-4a50-a6d9-b6a565ef2401"
        )

        update_event = {
            "body": json.dumps(common_test_helper.distribution_updated),
            "pathParameters": {
                "dataset-id": dataset_id,
                "version": common_test_helper.version["version"],
                "edition": "20190528T133700",
                "distribution": "e80b5f2c-67f0-4a50-a6d9-b6a565ef2401",
            },
        }

        response = distribution_handler.update_distribution(update_event, None)
        assert response["statusCode"] == 200
        assert json.loads(response["body"]) == distribution_id

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.DISTRIBUTION_ID).eq(distribution_id)
        )
        assert db_response["Items"][0]["filename"] == "UPDATED.csv"

    @mock_dynamodb2
    def test_get_all_distributions_new_table(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        distribution_table = common_test_helper.create_distribution_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        distribution_table.put_item(Item=common_test_helper.distribution)
        metadata_table.put_item(Item=common_test_helper.distribution_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version = common_test_helper.version["version"]
        get_all_event = {
            "pathParameters": {
                "dataset-id": dataset_id,
                "version": version,
                "edition": "20190528T133700",
            }
        }

        response = distribution_handler.get_distributions(get_all_event, None)
        distributions_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(distributions_from_db) == 1

        self_url = distributions_from_db[0].pop("_links")["self"]["href"]
        assert distributions_from_db[0] == common_test_helper.distribution_new_format
        assert (
            self_url
            == f"/datasets/{dataset_id}/versions/{version}/editions/20190528T133700/distributions/e80b5f2c-67f0-4a50-a6d9-b6a565ef2401"
        )

    @mock_dynamodb2
    def test_get_all_distributions_legacy(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        distribution_table = common_test_helper.create_distribution_table(dynamodb)
        distribution_table.put_item(Item=common_test_helper.distribution)
        distribution_table.put_item(Item=common_test_helper.distribution_updated)

        get_all_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.distribution[table.DATASET_ID],
                "version": common_test_helper.distribution[table.VERSION_ID],
                "edition": common_test_helper.distribution[table.EDITION_ID],
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

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version = common_test_helper.version["version"]
        distribution_id = common_test_helper.distribution_new_format[table.ID_COLUMN]
        get_event = {
            "pathParameters": {
                "dataset-id": dataset_id,
                "version": version,
                "edition": "20190528T133700",
                "distribution": "e80b5f2c-67f0-4a50-a6d9-b6a565ef2401",
            }
        }

        response = distribution_handler.get_distribution(get_event, None)
        distribution_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert distribution_from_db[table.ID_COLUMN] == distribution_id

        self_url = distribution_from_db.pop("_links")["self"]["href"]
        assert (
            self_url
            == f"/datasets/{dataset_id}/versions/{version}/editions/20190528T133700/distributions/e80b5f2c-67f0-4a50-a6d9-b6a565ef2401"
        )

    @mock_dynamodb2
    def test_get_one_distribution_legacy(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        distribution_table = common_test_helper.create_distribution_table(dynamodb)
        distribution_table.put_item(Item=common_test_helper.distribution)
        distribution_table.put_item(Item=common_test_helper.distribution_updated)

        distribution_id = common_test_helper.distribution[table.DISTRIBUTION_ID]
        get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.distribution[table.DATASET_ID],
                "version": common_test_helper.distribution[table.VERSION_ID],
                "edition": common_test_helper.distribution[table.EDITION_ID],
                "distribution": distribution_id,
            }
        }

        response = distribution_handler.get_distribution(get_event, None)
        distribution_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert distribution_from_db[table.DISTRIBUTION_ID] == distribution_id

    @mock_dynamodb2
    def test_distribution_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        common_test_helper.create_distribution_table(dynamodb)

        event_for_get = {
            "pathParameters": {
                "dataset-id": "1234",
                "version": "1",
                "edition": "20190401T133700",
                "distribution": "6f563c62-8fe4-4591-a999-5fbf0798e268",
            }
        }

        response = distribution_handler.get_distribution(event_for_get, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
