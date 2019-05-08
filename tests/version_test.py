import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import version_handler
import common_test_helper


class VersionTest(unittest.TestCase):
    @mock_dynamodb2
    def test_post_version(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        dataset_table = common_test_helper.create_dataset_table(dynamodb)
        version_table = common_test_helper.create_version_table(dynamodb)

        dataset_table.put_item(Item=common_test_helper.dataset)
        dataset_id = common_test_helper.dataset[table.DATASET_ID]

        version = common_test_helper.new_version
        create_event = {
            "body": json.dumps(version),
            "pathParameters": {"dataset-id": dataset_id},
        }

        response = version_handler.post_version(create_event, None)
        version_id = json.loads(response["body"])

        assert response["statusCode"] == 200

        db_response = version_table.query(
            KeyConditionExpression=Key(table.VERSION_ID).eq(version_id)
        )
        assert len(db_response["Items"]) == 1
        version_from_db = db_response["Items"][0]
        assert version_from_db["version"] == version["version"]
        assert version_from_db["datasetID"] == dataset_id
        assert version_from_db["versionID"].startswith("6-")

        bad_version_event = create_event
        bad_version_event["pathParameters"]["dataset-id"] = "ID NOT PRESENT"

        response = version_handler.post_version(bad_version_event, None)
        assert response["statusCode"] == 400

    @mock_dynamodb2
    def test_update_version(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        version_table = common_test_helper.create_version_table(dynamodb)
        version_table.put_item(Item=common_test_helper.version)

        dataset_id = common_test_helper.version[table.DATASET_ID]
        version_id = common_test_helper.version[table.VERSION_ID]

        update_event = {
            "body": json.dumps(common_test_helper.version_updated),
            "pathParameters": {"dataset-id": dataset_id, "version-id": version_id},
        }

        response = version_handler.update_version(update_event, None)
        assert response["statusCode"] == 200

        db_response = version_table.query(
            KeyConditionExpression=Key(table.VERSION_ID).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["version"] == "6-TEST"

    @mock_dynamodb2
    def test_get_all_versions(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        version_table = common_test_helper.create_version_table(dynamodb)
        version_table.put_item(Item=common_test_helper.version)
        version_table.put_item(Item=common_test_helper.version_updated)

        get_all_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.version[table.DATASET_ID]
            }
        }

        response = version_handler.get_versions(get_all_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 2

    @mock_dynamodb2
    def test_get_one_version(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        version_table = common_test_helper.create_version_table(dynamodb)
        version_table.put_item(Item=common_test_helper.version)

        version_id = common_test_helper.version[table.VERSION_ID]
        get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.version[table.DATASET_ID],
                "version-id": version_id,
            }
        }

        response = version_handler.get_version(get_event, None)
        version = json.loads(response["body"])

        assert version[table.VERSION_ID] == version_id

    @mock_dynamodb2
    def test_version_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_version_table(dynamodb)

        get_event = {"pathParameters": {"dataset-id": "1234", "version-id": "1"}}

        response = version_handler.get_version(get_event, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
