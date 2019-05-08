import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import edition_handler
import common_test_helper


class EditionTest(unittest.TestCase):
    @mock_dynamodb2
    def test_post_edition(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        dataset_table = common_test_helper.create_dataset_table(dynamodb)
        version_table = common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)

        dataset_table.put_item(Item=common_test_helper.dataset)
        version_table.put_item(Item=common_test_helper.version)

        create_event = {
            "body": json.dumps(common_test_helper.new_edition),
            "pathParameters": {
                "dataset-id": common_test_helper.edition[table.DATASET_ID],
                "version-id": common_test_helper.edition[table.VERSION_ID],
            },
        }

        response = edition_handler.post_edition(create_event, None)
        assert response["statusCode"] == 200

    @mock_dynamodb2
    def test_update_edition(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        edition_table = common_test_helper.create_edition_table(dynamodb)

        edition = common_test_helper.edition
        edition_id = edition[table.EDITION_ID]

        edition_table.put_item(Item=edition)

        update_event = {
            "body": json.dumps(common_test_helper.edition_updated),
            "pathParameters": {
                "dataset-id": edition[table.DATASET_ID],
                "version-id": edition[table.VERSION_ID],
                "edition-id": edition_id,
            },
        }

        response = edition_handler.update_edition(update_event, None)
        assert response["statusCode"] == 200

        db_response = edition_table.query(
            KeyConditionExpression=Key(table.EDITION_ID).eq(edition_id)
        )
        assert db_response["Items"][0]["description"] == "CHANGED"

    @mock_dynamodb2
    def test_get_all_editions(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        edition_table = common_test_helper.create_edition_table(dynamodb)

        edition = common_test_helper.edition
        edition_table.put_item(Item=edition)
        edition_table.put_item(Item=common_test_helper.edition_updated)

        get_all_event = {
            "pathParameters": {
                "dataset-id": edition[table.DATASET_ID],
                "version-id": edition[table.VERSION_ID],
            }
        }

        response = edition_handler.get_editions(get_all_event, None)

        response_body_as_json = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(response_body_as_json) == 2

    @mock_dynamodb2
    def test_should_fetch_edition_from_new_table_if_present(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        edition_table = common_test_helper.create_edition_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        edition_table.put_item(Item=common_test_helper.edition)
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.edition[table.DATASET_ID],
                "version-id": common_test_helper.version["version"],
                "edition-id": common_test_helper.edition["edition"],
            }
        }

        response = edition_handler.get_edition(get_event, None)
        edition_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert edition_from_db == common_test_helper.edition_new_format

    @mock_dynamodb2
    def test_get_one_edition_legacy(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        edition_table = common_test_helper.create_edition_table(dynamodb)

        edition = common_test_helper.edition
        edition_id = edition[table.EDITION_ID]

        edition_table.put_item(Item=edition)

        get_event = {
            "pathParameters": {
                "dataset-id": edition[table.DATASET_ID],
                "version-id": edition[table.VERSION_ID],
                "edition-id": edition_id,
            }
        }

        response = edition_handler.get_edition(get_event, None)
        edition_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert edition_from_db == edition

    @mock_dynamodb2
    def test_edition_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_edition_table(dynamodb)

        event_for_get = {
            "pathParameters": {
                "dataset-id": "1234",
                "version-id": "1",
                "edition-id": "20190401T133700",
            }
        }

        response = edition_handler.get_edition(event_for_get, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
