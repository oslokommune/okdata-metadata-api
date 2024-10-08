import pytest
import json

from boto3.dynamodb.conditions import Key

from metadata.CommonRepository import ID_COLUMN, TYPE_COLUMN
from metadata.version.handler import (
    create_version,
    delete_version,
    get_version,
    get_versions,
    update_version,
)
from tests import common_test_helper


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common_test_helper.create_metadata_table(dynamodb)


class TestCreateVersion:
    def test_create_version(self, metadata_table, auth_event, put_dataset):
        dataset_id = put_dataset

        version = common_test_helper.raw_version
        create_event = auth_event(version, dataset=dataset_id)

        response = create_version(create_event, None)
        body = json.loads(response["body"])
        version_id = body["Id"]

        expected_location = f'/datasets/{dataset_id}/versions/{version["version"]}'

        assert response["statusCode"] == 201
        assert response["headers"]["Location"] == expected_location
        assert version_id == f'{dataset_id}/{version["version"]}'

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(version_id)
        )
        items = db_response["Items"]

        assert len(items) == 1
        version_from_db = items[0]
        assert version_from_db[ID_COLUMN] == version_id
        assert version_from_db[TYPE_COLUMN] == "Version"

    def test_create_version_invalid_version_latest(self, auth_event):
        dataset_id = "my-dataset"
        version = {}
        version["version"] = "latest"
        create_event = auth_event(version, dataset=dataset_id)
        res = create_version(create_event, None)
        assert res["statusCode"] == 400
        body = json.loads(res["body"])
        assert body == {
            "message": "Validation error",
            "errors": [
                "version: 'latest' should not be valid under {'enum': ['latest']}"
            ],
        }

    def test_create_duplicate_version_should_fail(
        self, metadata_table, auth_event, put_dataset
    ):
        dataset_id = put_dataset

        version = common_test_helper.raw_version
        create_event = auth_event(version, dataset_id)

        response = create_version(create_event, None)
        assert response["statusCode"] == 201

        response = create_version(create_event, None)
        assert response["statusCode"] == 409
        body = json.loads(response["body"])
        assert str.startswith(body[0]["message"], "Resource Conflict")

    def test_forbidden(self, event, metadata_table, raw_dataset):
        raw_dataset[ID_COLUMN] = "dataset-id"
        raw_dataset[TYPE_COLUMN] = "Dataset"
        metadata_table.put_item(Item=raw_dataset)

        version = common_test_helper.raw_version
        create_event = event(version, "dataset-id")

        response = create_version(create_event, None)
        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {
                "message": f"You are not authorized to access dataset {raw_dataset[ID_COLUMN]}"
            }
        ]

    def test_daset_id_not_exist(self, auth_event, metadata_table):
        dataset_id = "dataset-id"
        create_event = auth_event(common_test_helper.raw_version, dataset=dataset_id)

        response = create_version(create_event, None)
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]


class TestUpdateVersion:
    def test_update_version(self, metadata_table, auth_event, put_dataset):
        dataset_id = put_dataset
        create_version(
            auth_event(common_test_helper.raw_version.copy(), dataset=dataset_id), None
        )
        version_name = common_test_helper.raw_version["version"]

        update_event = auth_event(
            common_test_helper.version_updated, dataset=dataset_id, version=version_name
        )

        response = update_version(update_event, None)
        body = json.loads(response["body"])
        version_id = body["Id"]

        assert response["statusCode"] == 200
        assert version_id == f"{dataset_id}/{version_name}"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(version_id)
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
        res = update_version(update_event, None)
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
        create_version(create_event, None)
        update_event = auth_event(
            common_test_helper.version_updated, dataset=dataset_id, version=version_name
        )
        update_version(update_event, None)

        version_id = "antall-besokende-pa-gjenbruksstasjoner/latest"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(version_id)
        )
        version_from_db = db_response["Items"][0]
        assert version_from_db["Id"] == "antall-besokende-pa-gjenbruksstasjoner/latest"
        assert version_from_db["latest"] == "antall-besokende-pa-gjenbruksstasjoner/6"

    def test_forbidden(self, event, metadata_table, put_dataset):
        version = common_test_helper.raw_version.copy()
        version[ID_COLUMN] = f"{put_dataset}/{version['version']}"
        version[TYPE_COLUMN] = "version"
        metadata_table.put_item(Item=version)

        dataset_id = put_dataset
        version_name = common_test_helper.raw_version["version"]

        update_event = event(
            common_test_helper.version_updated, dataset=dataset_id, version=version_name
        )
        response = update_version(update_event, None)

        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {"message": f"You are not authorized to access dataset {dataset_id}"}
        ]

    def test_daset_id_not_exist(self, auth_event, metadata_table):
        dataset_id = "dataset-id"
        update_event = auth_event(
            common_test_helper.version_updated, dataset=dataset_id, version="1"
        )
        response = update_version(update_event, None)
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]


class TestVersion:
    def test_get_all_versions(self, metadata_table, auth_event, put_dataset):
        dataset_id = put_dataset

        version_1 = common_test_helper.raw_version.copy()
        version_2 = common_test_helper.raw_version.copy()
        version_2["version"] = "extra-version"

        create_version(auth_event(version_1, dataset=dataset_id), None)
        create_version(auth_event(version_2, dataset=dataset_id), None)

        get_all_versions_event = auth_event({}, dataset=dataset_id)

        response = get_versions(get_all_versions_event, None)

        versions = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(versions) == 3  # Including initial version

    def test_version_not_found(self, event):
        get_event = event({}, "1234", "1")

        response = get_version(get_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == {"message": "Version not found."}


class TestDeleteVersion:
    def test_delete_ok(self, metadata_table, auth_event, put_version):
        dataset_id, version = put_version

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq("/".join(put_version))
        )
        assert db_response["Count"] == 1

        response = delete_version(auth_event(dataset=dataset_id, version=version), None)
        assert response["statusCode"] == 200

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq("/".join(put_version))
        )
        assert db_response["Count"] == 0

    def test_forbidden(self, metadata_table, auth_event, event, put_version):
        dataset_id, version = put_version

        response = delete_version(event(dataset=dataset_id, version=version), None)
        assert response["statusCode"] == 403

    def test_delete_not_found(self, auth_event):
        response = delete_version(auth_event(dataset="foo", version="1"), None)
        assert response["statusCode"] == 404
