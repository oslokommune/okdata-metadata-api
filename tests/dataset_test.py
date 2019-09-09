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


@pytest.fixture(autouse=True)
def dataset_table(dynamodb):
    return common.create_dataset_table(dynamodb)


class TestCreateDataset:
    def test_create(self, auth_event, metadata_table):
        create_event = auth_event(common.new_dataset)
        response = dataset_handler.create_dataset(create_event, None)
        dataset_id = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert response["headers"]["Location"] == f"/datasets/{dataset_id}"
        assert dataset_id == "antall-besokende-pa-gjenbruksstasjoner"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item[table.ID_COLUMN] == dataset_id
        assert item[table.TYPE_COLUMN] == "Dataset"
        assert item["title"] == "Antall besøkende på gjenbruksstasjoner"
        assert item["privacyLevel"] == "green"


class TestUpdateDataset:
    def test_update_dataset(self, auth_event, metadata_table):
        metadata_table.put_item(Item=common.dataset_new_format)

        dataset_id = common.dataset_new_format[table.ID_COLUMN]
        event_for_update = auth_event(common.dataset_updated, dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        body = json.loads(response["body"])

        assert body == dataset_id
        assert response["statusCode"] == 200

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item[table.ID_COLUMN] == dataset_id
        assert item[table.TYPE_COLUMN] == "Dataset"
        assert item["title"] == "UPDATED TITLE"
        assert item["privacyLevel"] == "red"

    def test_invalid_tokeN(self, event, metadata_table, auth_denied):
        metadata_table.put_item(Item=common.dataset_new_format)

        dataset_id = common.dataset_new_format[table.ID_COLUMN]
        event_for_update = event(common.dataset_updated, dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        assert response["statusCode"] == 403


class TestDataset:
    def test_should_just_get_datasets_from_new_table(
        self, metadata_table, dataset_table
    ):
        dataset_table.put_item(Item=common.dataset_updated)
        metadata_table.put_item(Item=common.dataset_new_format)

        response = dataset_handler.get_datasets(None, None)
        datasets = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(datasets) == 1

    def test_should_not_get_datasets_legacy(self, dataset_table):
        dataset_table.put_item(Item=common.dataset)
        dataset_table.put_item(Item=common.dataset_updated)

        response = dataset_handler.get_datasets(None, None)
        datasets = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(datasets) == 0

    def test_should_fetch_dataset_from_new_table_if_present(
        self, metadata_table, dataset_table, event
    ):
        dataset_id = common.dataset[table.DATASET_ID]
        dataset_table.put_item(Item=common.dataset)
        metadata_table.put_item(Item=common.dataset_new_format)

        get_event = event({}, dataset_id)

        response = dataset_handler.get_dataset(get_event, None)
        dataset_from_db = json.loads(response["body"])

        self_url = dataset_from_db.pop("_links")["self"]["href"]
        assert self_url == f"/datasets/{dataset_id}"
        assert dataset_from_db == common.dataset_new_format

    def test_get_dataset_from_legacy_table(self, dataset_table, event):
        dataset_table.put_item(Item=common.dataset)

        dataset_id = common.dataset[table.DATASET_ID]
        event_for_get = event({}, dataset_id)

        response = dataset_handler.get_dataset(event_for_get, None)
        dataset_from_db = json.loads(response["body"])

        assert dataset_from_db == common.dataset

    def test_dataset_not_found(self, event):
        event_for_get = event({}, "1234")

        response = dataset_handler.get_dataset(event_for_get, None)

        assert response["statusCode"] == 404

    def test_slugify(self):
        title = (
            "  Tittel på datasett 42 med spesialtegn :+*/\\_[](){} og norske tegn ÆØÅ  "
        )
        result = dataset_repository.slugify(title)

        assert result == "tittel-pa-datasett-42-med-spesialtegn-og-norske-tegn-eoa"
