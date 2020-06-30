import pytest
import json

from boto3.dynamodb.conditions import Key

import metadata.common as table
import metadata.dataset.handler as dataset_handler
import metadata.dataset.repository as dataset_repository
import tests.common_test_helper as common


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common.create_metadata_table(dynamodb)


class TestCreateDataset:
    def test_create(self, auth_event, metadata_table):
        create_event = auth_event(common.raw_dataset.copy())
        response = dataset_handler.create_dataset(create_event, None)
        body = json.loads(response["body"])
        dataset_id = body["Id"]
        assert response["statusCode"] == 201
        assert response["headers"]["Location"] == f"/datasets/{dataset_id}"
        assert dataset_id == "antall-besokende-pa-gjenbruksstasjoner"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item[table.ID_COLUMN] == dataset_id
        assert item[table.TYPE_COLUMN] == "Dataset"
        assert item["title"] == "Antall besøkende på gjenbruksstasjoner"
        assert item["confidentiality"] == "green"

    def test_create_invalid(self, auth_event, metadata_table):
        invalid_dataset = common.raw_dataset.copy()
        invalid_dataset["confidentiality"] = "blue"
        create_event = auth_event(invalid_dataset)
        response = dataset_handler.create_dataset(create_event, None)
        error_message = json.loads(response["body"])
        assert response["statusCode"] == 400
        assert error_message == {
            "message": "Validation error",
            "errors": [
                "confidentiality: 'blue' is not one of ['green', 'yellow', 'red', 'purple']"
            ],
        }


class TestUpdateDataset:
    def test_update_dataset(self, auth_event, metadata_table):
        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]
        event_for_update = auth_event(common.dataset_updated.copy(), dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        body = json.loads(response["body"])

        assert body["Id"] == dataset_id
        assert response["statusCode"] == 200

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item[table.ID_COLUMN] == dataset_id
        assert item[table.TYPE_COLUMN] == "Dataset"
        assert item["title"] == "UPDATED TITLE"
        assert item["confidentiality"] == "red"

    def test_forbidden(self, event, metadata_table, auth_event, auth_denied):
        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]
        event_for_update = event(common.dataset_updated, dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        assert response["statusCode"] == 403

    def test_update_invalid(self, auth_event, metadata_table):
        dataset = common.raw_dataset.copy()
        dataset_handler.create_dataset(auth_event(dataset), None)

        invalid_dataset = dataset.copy()
        invalid_dataset["confidentiality"] = "blue"
        update_event = auth_event(invalid_dataset)

        response = dataset_handler.update_dataset(update_event, None)
        error_message = json.loads(response["body"])
        assert response["statusCode"] == 400
        assert error_message == {
            "message": "Validation error",
            "errors": [
                "confidentiality: 'blue' is not one of ['green', 'yellow', 'red', 'purple']"
            ],
        }

    def test_dataset_not_exist(self, auth_event, metadata_table):
        dataset_id = "dataset-id"
        event_for_update = auth_event(common.dataset_updated, dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]

    def test_slugify(self):
        title = (
            "  Tittel på datasett 42 med spesialtegn :+*/\\_[](){} og norske tegn ÆØÅ  "
        )
        result = dataset_repository.slugify(title)

        assert result == "tittel-pa-datasett-42-med-spesialtegn-og-norske-tegn-eoa"
