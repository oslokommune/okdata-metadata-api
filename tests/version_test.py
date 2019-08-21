from copy import deepcopy
import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import metadata.common as table
import metadata.version.handler as version_handler
from tests import common_test_helper


class VersionTest(unittest.TestCase):
    @mock_dynamodb2
    def test_create_version(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_version_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        dataset = common_test_helper.dataset_new_format
        dataset_id = dataset[table.ID_COLUMN]
        metadata_table.put_item(Item=dataset)

        version = common_test_helper.new_version
        create_event = {
            "body": json.dumps(version),
            "pathParameters": {"dataset-id": dataset_id},
        }

        response = version_handler.create_version(create_event, None)
        version_id = json.loads(response["body"])

        expected_location = f'/datasets/{dataset_id}/versions/{version["version"]}'

        assert response["statusCode"] == 200
        assert response["headers"]["Location"] == expected_location
        assert version_id == f'{dataset_id}/{version["version"]}'

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(version_id)
        )
        items = db_response["Items"]

        assert len(items) == 1
        version_from_db = items[0]
        assert version_from_db[table.ID_COLUMN] == version_id
        assert version_from_db[table.TYPE_COLUMN] == "Version"

        bad_version_event = create_event
        bad_version_event["pathParameters"]["dataset-id"] = "ID NOT PRESENT"

        response = version_handler.create_version(bad_version_event, None)
        assert response["statusCode"] == 400

    @mock_dynamodb2
    def test_create_duplicate_version_should_fail(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_version_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        dataset = common_test_helper.dataset_new_format
        dataset_id = dataset[table.ID_COLUMN]
        metadata_table.put_item(Item=dataset)

        version = common_test_helper.new_version
        create_event = {
            "body": json.dumps(version),
            "pathParameters": {"dataset-id": dataset_id},
        }

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 200

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 409
        assert str.startswith(json.loads(response["body"]), "Resource Conflict")

    @mock_dynamodb2
    def test_update_version(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version_name = common_test_helper.version_new_format["version"]

        create_event = {
            "body": json.dumps(common_test_helper.version_new_format),
            "pathParameters": {"dataset-id": dataset_id, "version": version_name},
        }

        # Insert parent first:
        metadata_table.put_item(Item=common_test_helper.dataset_new_format)
        version_handler.create_version(create_event, None)
        update_event = {
            "body": json.dumps(common_test_helper.version_updated),
            "pathParameters": {"dataset-id": dataset_id, "version": version_name},
        }

        response = version_handler.update_version(update_event, None)
        version_id = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert version_id == f"{dataset_id}/{version_name}"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["schema"] == "new schema"

    @mock_dynamodb2
    def test_update_edition_latest_is_updated(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version_name = common_test_helper.version_new_format["version"]
        create_event = {
            "body": json.dumps(common_test_helper.version_new_format),
            "pathParameters": {"dataset-id": dataset_id, "version": version_name},
        }

        # Insert parent first:
        metadata_table.put_item(Item=common_test_helper.dataset_new_format)
        version_handler.create_version(create_event, None)

        update_event = {
            "body": json.dumps(common_test_helper.version_updated),
            "pathParameters": {"dataset-id": dataset_id, "version": version_name},
        }
        version_handler.update_version(update_event, None)

        version_id = f"antall-besokende-pa-gjenbruksstasjoner/latest"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["Id"] == "antall-besokende-pa-gjenbruksstasjoner/latest"
        assert version_from_db["latest"] == "antall-besokende-pa-gjenbruksstasjoner/6"

    @mock_dynamodb2
    def test_should_get_versions_from_new_table_if_present(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        version_table = common_test_helper.create_version_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        version_table.put_item(Item=common_test_helper.version_updated)
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.version[table.DATASET_ID]
        get_all_event = {"pathParameters": {"dataset-id": dataset_id}}

        response = version_handler.get_versions(get_all_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 1

        self_url = versions[0].pop("_links")["self"]["href"]
        assert self_url == f"/datasets/{dataset_id}/versions/6"
        assert versions[0] == common_test_helper.version_new_format

    @mock_dynamodb2
    def test_should_get_versions_from_legacy_table_if_not_present_in_new_table(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        version_table = common_test_helper.create_version_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        version_new_table = deepcopy(common_test_helper.version_new_format)
        version_new_table[table.ID_COLUMN] = "my-dataset/6"

        version_table.put_item(Item=common_test_helper.version)
        metadata_table.put_item(Item=version_new_table)

        get_all_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.version[table.DATASET_ID]
            }
        }

        response = version_handler.get_versions(get_all_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 1
        assert versions[0] == common_test_helper.version

    @mock_dynamodb2
    def test_get_all_versions(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_version_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)
        metadata_table.put_item(Item=common_test_helper.version_new_format)
        metadata_table.put_item(Item=common_test_helper.next_version_new_format)

        get_all_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.dataset_new_format[table.ID_COLUMN]
            }
        }

        response = version_handler.get_versions(get_all_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 2

    @mock_dynamodb2
    def test_should_fetch_version_from_new_metadata_table_if_present(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        version_table = common_test_helper.create_version_table(dynamodb)
        metadata_table = common_test_helper.create_metadata_table(dynamodb)

        version_table.put_item(Item=common_test_helper.version)
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.version[table.DATASET_ID]
        version_name = common_test_helper.version["version"]
        get_event = {
            "pathParameters": {"dataset-id": dataset_id, "version": version_name}
        }

        response = version_handler.get_version(get_event, None)

        version_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert (
            version_from_db[table.ID_COLUMN]
            == common_test_helper.version_new_format[table.ID_COLUMN]
        )
        assert version_from_db[table.TYPE_COLUMN] == "Version"

        self_url = version_from_db["_links"]["self"]["href"]
        assert self_url == f"/datasets/{dataset_id}/versions/{version_name}"

    @mock_dynamodb2
    def test_get_version_from_legacy_table(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        version_table = common_test_helper.create_version_table(dynamodb)
        version_table.put_item(Item=common_test_helper.version)

        version_id = common_test_helper.version[table.VERSION_ID]
        get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.version[table.DATASET_ID],
                "version": version_id,
            }
        }

        response = version_handler.get_version(get_event, None)
        version_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert version_from_db[table.VERSION_ID] == version_id

    @mock_dynamodb2
    def test_version_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_metadata_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)

        get_event = {"pathParameters": {"dataset-id": "1234", "version": "1"}}

        response = version_handler.get_version(get_event, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
