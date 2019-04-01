# -*- coding: utf-8 -*-

import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import dataset_handler
import test_dir.common_test_helper as common


class DatasetTest(unittest.TestCase):
    @mock_dynamodb2
    def test_post_dataset(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')
        common.create_dataset_table(dynamodb)

        event = common.dataset_event

        response = dataset_handler.post_dataset(event, None)

        assert response["statusCode"] == 200

    @mock_dynamodb2
    def test_update_dataset(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')

        dataset_table = common.create_dataset_table(dynamodb)

        event = common.dataset_event

        response_from_post = dataset_handler.post_dataset(event, None)
        response_from_first_post_as_json = json.loads(response_from_post["body"])

        event_for_update = common.dataset_event_updated
        event_for_update["pathParameters"] = {
            "dataset-id": response_from_first_post_as_json
        }

        assert response_from_post["statusCode"] == 200
        response_from_post = dataset_handler.update_dataset(event_for_update, None)
        response_from_second_post_as_json = json.loads(response_from_post["body"])

        assert response_from_first_post_as_json == response_from_second_post_as_json
        assert response_from_post["statusCode"] == 200

        db_response = dataset_table.query(
            KeyConditionExpression=Key(table.DATASET_ID).eq(response_from_first_post_as_json)

        )
        assert db_response["Items"][0]["privacyLevel"] == "red"

    @mock_dynamodb2
    def test_get_all_datasets(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')

        common.create_dataset_table(dynamodb)

        event1 = common.dataset_event

        event2 = common.dataset_event_updated

        dataset_handler.post_dataset(event1, None)
        dataset_handler.post_dataset(event2, None)

        response = dataset_handler.get_datasets(None, None)

        response_body_as_json = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(response_body_as_json) == 2

    @mock_dynamodb2
    def test_get_one_dataset(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')

        common.create_dataset_table(dynamodb)

        event_for_post = common.dataset_event

        response_from_post = dataset_handler.post_dataset(event_for_post, None)["body"]
        response_from_post_as_json = json.loads(response_from_post)

        event_for_get = {
            "pathParameters": {
                "dataset-id": response_from_post_as_json
            }
        }

        response_from_get_as_json = json.loads(dataset_handler.get_dataset(event_for_get, None)["body"])

        assert response_from_get_as_json[table.DATASET_ID] == response_from_post_as_json


if __name__ == '__main__':
    unittest.main()
