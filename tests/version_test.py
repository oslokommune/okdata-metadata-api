import pytest
from copy import deepcopy
import json

from boto3.dynamodb.conditions import Key

import metadata.common as table
import metadata.version.handler as version_handler
from tests import common_test_helper


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common_test_helper.create_metadata_table(dynamodb)


@pytest.fixture(autouse=True)
def version_table(dynamodb):
    return common_test_helper.create_version_table(dynamodb)


class TestCreateVersion:
    def test_create_version(self, metadata_table, auth_event):
        dataset = common_test_helper.dataset_new_format
        dataset_id = dataset[table.ID_COLUMN]
        metadata_table.put_item(Item=dataset)

        version = common_test_helper.new_version
        create_event = auth_event(version, dataset_id)

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

    def test_create_duplicate_version_should_fail(self, metadata_table, auth_event):
        dataset = common_test_helper.dataset_new_format
        dataset_id = dataset[table.ID_COLUMN]
        metadata_table.put_item(Item=dataset)

        version = common_test_helper.new_version
        create_event = auth_event(version, dataset_id)

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 200

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 409
        assert str.startswith(json.loads(response["body"]), "Resource Conflict")

    def test_forbidden(self, event, metadata_table):
        dataset = common_test_helper.dataset_new_format
        dataset_id = dataset[table.ID_COLUMN]
        metadata_table.put_item(Item=dataset)

        version = common_test_helper.new_version
        create_event = event(version, dataset_id)

        response = version_handler.create_version(create_event, None)
        assert response["statusCode"] == 403


class TestUpdateVersion:
    def test_update_version(self, metadata_table, auth_event):
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version_name = common_test_helper.version_new_format["version"]

        update_event = auth_event(
            common_test_helper.version_updated, dataset_id, version_name
        )

        response = version_handler.update_version(update_event, None)
        version_id = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert version_id == f"{dataset_id}/{version_name}"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["schema"] == "new schema"

    def test_update_edition_latest_is_updated(self, metadata_table, auth_event):
        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version_name = common_test_helper.version_new_format["version"]
        create_event = auth_event(
            common_test_helper.version_new_format, dataset_id, version_name
        )
        print(f"create_event: {create_event}")
        # Insert parent first:
        metadata_table.put_item(Item=common_test_helper.dataset_new_format)
        version_handler.create_version(create_event, None)
        update_event = auth_event(
            common_test_helper.version_updated, dataset_id, version_name
        )
        version_handler.update_version(update_event, None)

        version_id = f"antall-besokende-pa-gjenbruksstasjoner/latest"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["Id"] == "antall-besokende-pa-gjenbruksstasjoner/latest"
        assert version_from_db["latest"] == "antall-besokende-pa-gjenbruksstasjoner/6"

    def test_forbidden(self, event, metadata_table):
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version_name = common_test_helper.version_new_format["version"]

        update_event = event(
            common_test_helper.version_updated, dataset_id, version_name
        )
        response = version_handler.update_version(update_event, None)

        assert response["statusCode"] == 403


class TestVersion:
    def test_should_get_versions_from_new_table_if_present(
        self, metadata_table, version_table, event
    ):
        version_table.put_item(Item=common_test_helper.version_updated)
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.version[table.DATASET_ID]
        get_all_event = event({}, dataset_id)

        response = version_handler.get_versions(get_all_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 1

        self_url = versions[0].pop("_links")["self"]["href"]
        assert self_url == f"/datasets/{dataset_id}/versions/6"
        assert versions[0] == common_test_helper.version_new_format

    def test_should_get_versions_from_legacy_table_if_not_present_in_new_table(
        self, metadata_table, version_table, event
    ):
        version_new_table = deepcopy(common_test_helper.version_new_format)
        version_new_table[table.ID_COLUMN] = "my-dataset/6"

        version_table.put_item(Item=common_test_helper.version)
        metadata_table.put_item(Item=version_new_table)

        get_all_event = event({}, common_test_helper.version[table.DATASET_ID])

        response = version_handler.get_versions(get_all_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 1
        assert versions[0] == common_test_helper.version

    def test_get_all_versions(self, metadata_table, event):
        metadata_table.put_item(Item=common_test_helper.version_new_format)
        metadata_table.put_item(Item=common_test_helper.next_version_new_format)

        get_all_event = event(
            {}, common_test_helper.dataset_new_format[table.ID_COLUMN]
        )

        response = version_handler.get_versions(get_all_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 2

    def test_should_fetch_version_from_new_metadata_table_if_present(
        self, metadata_table, version_table, event
    ):
        version_table.put_item(Item=common_test_helper.version)
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.version[table.DATASET_ID]
        version_name = common_test_helper.version["version"]
        get_event = event({}, dataset_id, version_name)

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

    def test_get_version_from_legacy_table(self, version_table, event):
        version_table.put_item(Item=common_test_helper.version)

        version_id = common_test_helper.version[table.VERSION_ID]
        get_event = event({}, common_test_helper.version[table.DATASET_ID], version_id)

        response = version_handler.get_version(get_event, None)
        version_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert version_from_db[table.VERSION_ID] == version_id

    def test_version_not_found(self, event):
        get_event = event({}, "1234", "1")

        response = version_handler.get_version(get_event, None)

        assert response["statusCode"] == 404
