# -*- coding: utf-8 -*-

import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import dataset_handler
import dataset_repository
import common_test_helper as common


class DatasetTest(unittest.TestCase):
    @mock_dynamodb2
    def test_create_dataset(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common.create_dataset_table(dynamodb)
        metadata_table = common.create_metadata_table(dynamodb)

        create_event = {"body": json.dumps(common.new_dataset)}
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

    @mock_dynamodb2
    def test_update_dataset(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        metadata_table = common.create_metadata_table(dynamodb)
        metadata_table.put_item(Item=common.dataset_new_format)

        dataset_id = common.dataset_new_format[table.ID_COLUMN]
        event_for_update = {
            "body": json.dumps(common.dataset_updated),
            "pathParameters": {"dataset-id": dataset_id},
        }

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

    @mock_dynamodb2
    def test_should_get_datasets_from_both_tables(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        dataset_table = common.create_dataset_table(dynamodb)
        metadata_table = common.create_metadata_table(dynamodb)
        dataset_table.put_item(Item=common.dataset_updated)
        metadata_table.put_item(Item=common.dataset_new_format)

        response = dataset_handler.get_datasets(None, None)
        datasets = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(datasets) == 2
        assert common.dataset_new_format in datasets
        assert common.dataset_updated in datasets

    @mock_dynamodb2
    def test_get_all_datasets_legacy(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common.create_metadata_table(dynamodb)

        dataset_table = common.create_dataset_table(dynamodb)
        dataset_table.put_item(Item=common.dataset)
        dataset_table.put_item(Item=common.dataset_updated)

        response = dataset_handler.get_datasets(None, None)
        datasets = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(datasets) == 2

    @mock_dynamodb2
    def test_should_fetch_dataset_from_new_table_if_present(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        dataset_table = common.create_dataset_table(dynamodb)
        metadata_table = common.create_metadata_table(dynamodb)

        dataset_id = common.dataset[table.DATASET_ID]
        dataset_table.put_item(Item=common.dataset)
        metadata_table.put_item(Item=common.dataset_new_format)

        get_event = {"pathParameters": {"dataset-id": dataset_id}}

        response = dataset_handler.get_dataset(get_event, None)
        dataset_from_db = json.loads(response["body"])

        assert dataset_from_db == common.dataset_new_format

    @mock_dynamodb2
    def test_get_dataset_from_legacy_table(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        common.create_metadata_table(dynamodb)
        dataset_table = common.create_dataset_table(dynamodb)
        dataset_table.put_item(Item=common.dataset)

        dataset_id = common.dataset[table.DATASET_ID]
        event_for_get = {"pathParameters": {"dataset-id": dataset_id}}

        response = dataset_handler.get_dataset(event_for_get, None)
        dataset_from_db = json.loads(response["body"])

        assert dataset_from_db == common.dataset

    @mock_dynamodb2
    def test_dataset_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common.create_metadata_table(dynamodb)
        common.create_dataset_table(dynamodb)

        event_for_get = {"pathParameters": {"dataset-id": "1234"}}

        response = dataset_handler.get_dataset(event_for_get, None)

        assert response["statusCode"] == 404

    def test_slugify(self):
        title = (
            "  Tittel på datasett 42 med spesialtegn :+*/\\_[](){} og norske tegn ÆØÅ  "
        )
        result = dataset_repository.slugify(title)

        assert result == "tittel-pa-datasett-42-med-spesialtegn-og-norske-tegn-eoa"


if __name__ == "__main__":
    unittest.main()
