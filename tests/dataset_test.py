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
    def test_post_dataset(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        dataset_table = common.create_dataset_table(dynamodb)

        create_event = {"body": json.dumps(common.new_dataset)}
        response = dataset_handler.post_dataset(create_event, None)
        dataset_id = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert dataset_id == "antall-besokende-pa-gjenbruksstasjoner"

        db_response = dataset_table.query(
            KeyConditionExpression=Key(table.DATASET_ID).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item["title"] == "Antall besøkende på gjenbruksstasjoner"
        assert item["privacyLevel"] == "green"

    @mock_dynamodb2
    def test_update_dataset(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

        dataset_table = common.create_dataset_table(dynamodb)
        dataset_table.put_item(Item=common.dataset)

        dataset_id = common.dataset[table.DATASET_ID]
        event_for_update = {
            "body": json.dumps(common.dataset_updated),
            "pathParameters": {"dataset-id": dataset_id},
        }

        response = dataset_handler.update_dataset(event_for_update, None)
        body = json.loads(response["body"])

        assert body == dataset_id
        assert response["statusCode"] == 200

        db_response = dataset_table.query(
            KeyConditionExpression=Key(table.DATASET_ID).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item["title"] == "UPDATED TITLE"
        assert item["privacyLevel"] == "red"

    @mock_dynamodb2
    def test_get_all_datasets(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")

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
