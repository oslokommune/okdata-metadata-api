import pytest
import json

from boto3.dynamodb.conditions import Key

import metadata.common as table
import metadata.version.handler as version_handler
from metadata import common
from tests import common_test_helper


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common_test_helper.create_metadata_table(dynamodb)


class TestCreateVersion:
    def test_create_version(self, metadata_table, auth_event, put_dataset):
        dataset_id = put_dataset

        version = common_test_helper.raw_version
        create_event = auth_event(version, dataset=dataset_id)

        response = version_handler.create_version(create_event, None)
        body = json.loads(response["body"])
        version_id = body["Id"]

        expected_location = f'/datasets/{dataset_id}/versions/{version["version"]}'

        assert response["statusCode"] == 201
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

        response = version_handler.create_version(
            bad_version_event, common_test_helper.Context("1234")
        )
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": "Dataset ID NOT PRESENT does not exist"}
        ]

    def test_create_version_invalid_version_latest(self, auth_event):
        dataset_id = "my-dataset"
        version = {}
        version["version"] = "latest"
        create_event = auth_event(version, dataset=dataset_id)
        res = version_handler.create_version(create_event, None)
        assert res["statusCode"] == 400
        body = json.loads(res["body"])
        assert body == {
            "message": "Validation error",
            "errors": ["version: {'enum': ['latest']} is not allowed for 'latest'"],
        }

    def test_create_duplicate_version_should_fail(
        self, metadata_table, auth_event, put_dataset
    ):
        dataset_id = put_dataset

        version = common_test_helper.raw_version
        create_event = auth_event(version, dataset_id)

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 201

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 409
        body = json.loads(response["body"])
        assert str.startswith(body[0]["message"], "Resource Conflict")

    def test_forbidden(self, event, metadata_table, auth_denied):
        dataset = common_test_helper.raw_dataset.copy()
        dataset[table.ID_COLUMN] = "dataset-id"
        dataset[table.TYPE_COLUMN] = "Dataset"
        metadata_table.put_item(Item=dataset)

        version = common_test_helper.raw_version
        create_event = event(version, "dataset-id")

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 403


class TestUpdateVersion:
    def test_update_version(self, metadata_table, auth_event, put_dataset):
        dataset_id = put_dataset
        version_handler.create_version(
            auth_event(common_test_helper.raw_version.copy(), dataset=dataset_id), None
        )
        version_name = common_test_helper.raw_version["version"]

        update_event = auth_event(
            common_test_helper.version_updated, dataset=dataset_id, version=version_name
        )

        response = version_handler.update_version(update_event, None)
        body = json.loads(response["body"])
        version_id = body["Id"]

        assert response["statusCode"] == 200
        assert version_id == f"{dataset_id}/{version_name}"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["version"] == "6"

    def test_update_version_invalid_version_latest_in_body(
        self, auth_event, put_dataset
    ):
        dataset_id = put_dataset
        version_name = common_test_helper.raw_version["version"]
        update_event = auth_event(
            common_test_helper.version_updated, dataset=dataset_id, version=version_name
        )
        body = json.loads(update_event["body"])
        body["version"] = "this-is-not-the-same-version"
        update_event["body"] = json.dumps(body)
        res = version_handler.update_version(update_event, None)
        assert res["statusCode"] == 409

    def test_update_edition_latest_is_updated(
        self, metadata_table, auth_event, put_dataset
    ):
        dataset_id = put_dataset
        version_name = common_test_helper.raw_version["version"]
        create_event = auth_event(
            common_test_helper.raw_version, dataset=dataset_id, version=version_name
        )
        # Insert parent first:
        version_handler.create_version(create_event, None)
        update_event = auth_event(
            common_test_helper.version_updated, dataset=dataset_id, version=version_name
        )
        version_handler.update_version(update_event, None)

        version_id = "antall-besokende-pa-gjenbruksstasjoner/latest"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["Id"] == "antall-besokende-pa-gjenbruksstasjoner/latest"
        assert version_from_db["latest"] == "antall-besokende-pa-gjenbruksstasjoner/6"

    def test_forbidden(self, event, metadata_table, put_dataset, auth_denied):
        version = common_test_helper.raw_version.copy()
        version[common.ID_COLUMN] = f"{put_dataset}/{version['version']}"
        version[common.TYPE_COLUMN] = "version"
        metadata_table.put_item(Item=version)

        dataset_id = put_dataset
        version_name = common_test_helper.raw_version["version"]

        update_event = event(
            common_test_helper.version_updated, dataset=dataset_id, version=version_name
        )
        response = version_handler.update_version(update_event, None)

        assert response["statusCode"] == 403


class TestVersion:
    def test_get_all_versions(self, metadata_table, auth_event, put_dataset):
        dataset_id = put_dataset

        version_1 = common_test_helper.raw_version.copy()
        version_2 = common_test_helper.raw_version.copy()
        version_2["version"] = "extra-version"

        version_handler.create_version(auth_event(version_1, dataset=dataset_id), None)
        version_handler.create_version(auth_event(version_2, dataset=dataset_id), None)

        get_all_versions_event = auth_event({}, dataset=dataset_id)

        response = version_handler.get_versions(get_all_versions_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 2

    def test_version_not_found(self, event):
        get_event = event({}, "1234", "1")

        response = version_handler.get_version(get_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == {"message": "Version not found."}
