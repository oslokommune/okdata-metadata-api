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
    def test_create_edition(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_edition_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]

        create_event = {
            "body": json.dumps(common_test_helper.new_edition),
            "pathParameters": {
                "dataset-id": dataset_id,
                "version": common_test_helper.version_new_format["version"],
            },
        }

        response = edition_handler.create_edition(create_event, None)
        edition_id = json.loads(response["body"])

        expected_location = f"/datasets/{dataset_id}/versions/6/editions/1557273600"

        assert response["statusCode"] == 200
        assert response["headers"]["Location"] == expected_location
        assert edition_id == f"{dataset_id}/6/1557273600"

    @mock_dynamodb2
    def test_create_duplicate_edition_should_fail(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_edition_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        metadata_table.put_item(Item=common_test_helper.dataset_new_format)
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        create_event = {
            "body": json.dumps(common_test_helper.new_edition),
            "pathParameters": {
                "dataset-id": common_test_helper.dataset_new_format[table.ID_COLUMN],
                "version": common_test_helper.version_new_format["version"],
            },
        }

        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 200

        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 409
        assert str.startswith(json.loads(response["body"]), "Resource Conflict")

    @mock_dynamodb2
    def test_update_edition(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        metadata_table = common_test_helper.create_metadata_table(dynamodb)
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        update_event = {
            "body": json.dumps(common_test_helper.edition_updated),
            "pathParameters": {
                "dataset-id": "antall-besokende-pa-gjenbruksstasjoner",
                "version": "6",
                "edition": "1557273600",
            },
        }

        response = edition_handler.update_edition(update_event, None)
        edition_id = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert edition_id == f"antall-besokende-pa-gjenbruksstasjoner/6/1557273600"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(edition_id)
        )
        edition_from_db = db_response["Items"][0]

        assert edition_from_db["edition"] == "1557273600"
        assert edition_from_db["description"] == "CHANGED"

    @mock_dynamodb2
    def test_get_all_editions_from_new_table_if_present(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        edition_table = common_test_helper.create_edition_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        edition_table.put_item(Item=common_test_helper.edition)
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        get_all_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.dataset[table.DATASET_ID],
                "version": common_test_helper.version["version"],
            }
        }

        response = edition_handler.get_editions(get_all_event, None)

        editions_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(editions_from_db) == 1
        assert editions_from_db[0] == common_test_helper.edition_new_format

    @mock_dynamodb2
    def test_get_all_editions_legacy(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        edition_table = common_test_helper.create_edition_table(dynamodb)

        edition = common_test_helper.edition
        edition_table.put_item(Item=edition)
        edition_table.put_item(Item=common_test_helper.edition_updated)

        get_all_event = {
            "pathParameters": {
                "dataset-id": edition[table.DATASET_ID],
                "version": edition[table.VERSION_ID],
            }
        }

        response = edition_handler.get_editions(get_all_event, None)

        editions_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(editions_from_db) == 2

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
                "version": common_test_helper.version["version"],
                "edition": common_test_helper.edition["edition"],
            }
        }

        response = edition_handler.get_edition(get_event, None)
        edition_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert edition_from_db == common_test_helper.edition_new_format

    @mock_dynamodb2
    def test_get_one_edition_legacy(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        edition_table = common_test_helper.create_edition_table(dynamodb)

        edition = common_test_helper.edition
        edition_id = edition[table.EDITION_ID]

        edition_table.put_item(Item=edition)

        get_event = {
            "pathParameters": {
                "dataset-id": edition[table.DATASET_ID],
                "version": edition[table.VERSION_ID],
                "edition": edition_id,
            }
        }

        response = edition_handler.get_edition(get_event, None)
        edition_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert edition_from_db == edition

    @mock_dynamodb2
    def test_edition_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)

        event_for_get = {
            "pathParameters": {
                "dataset-id": "1234",
                "version": "1",
                "edition": "20190401T133700",
            }
        }

        response = edition_handler.get_edition(event_for_get, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
