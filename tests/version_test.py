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
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)

        _, version_response = common_test_helper.post_version(
            common_test_helper.dataset_event, common_test_helper.version_event
        )
        version_response = version_response[0]
        assert version_response["statusCode"] == 200

        bad_version_event = common_test_helper.version_event
        bad_version_event["pathParameters"] = {"dataset-id": "ID NOT PRESENT"}

        response = version_handler.post_version(bad_version_event, None)

        assert response["statusCode"] == 404

    @mock_dynamodb2
    def test_update_version(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        version_table = common_test_helper.create_version_table(dynamodb)

        response_from_dataset_post, response_from_version_post = common_test_helper.post_version(
            common_test_helper.dataset_event, common_test_helper.version_event
        )
        response_from_version_post = response_from_version_post[0]

        assert response_from_version_post["statusCode"] == 200

        event_for_version_put = common_test_helper.version_event_updated
        event_for_version_put["pathParameters"] = {
            "dataset-id": common_test_helper.read_result_body(
                response_from_dataset_post
            ),
            "version-id": common_test_helper.read_result_body(
                response_from_version_post
            ),
        }

        response_from_put = version_handler.update_version(event_for_version_put, None)

        assert response_from_put["statusCode"] == 200
        db_response = version_table.query(
            KeyConditionExpression=Key(table.VERSION_ID).eq(
                common_test_helper.read_result_body(response_from_version_post)
            )
        )
        assert db_response["Items"][0]["version"] == "6-TEST"

    @mock_dynamodb2
    def test_get_all_versions(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)

        response_from_dataset_post, _ = common_test_helper.post_version(
            common_test_helper.dataset_event,
            common_test_helper.version_event,
            common_test_helper.version_event_updated,
        )

        path_parameter_for_get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.read_result_body(
                    response_from_dataset_post
                )
            }
        }

        response = version_handler.get_versions(path_parameter_for_get_event, None)

        response_body_as_json = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(response_body_as_json) == 2

    @mock_dynamodb2
    def test_get_one_version(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)

        response_from_dataset_post, response_from_post_as_json = common_test_helper.post_version(
            common_test_helper.dataset_event, common_test_helper.version_event
        )
        response_from_post_as_json = response_from_post_as_json[0]
        path_parameter_for_get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.read_result_body(
                    response_from_dataset_post
                ),
                "version-id": common_test_helper.read_result_body(
                    response_from_post_as_json
                ),
            }
        }

        response_from_get_as_json = common_test_helper.read_result_body(
            version_handler.get_version(path_parameter_for_get_event, None)
        )

        assert response_from_get_as_json[
            table.VERSION_ID
        ] == common_test_helper.read_result_body(response_from_post_as_json)

    @mock_dynamodb2
    def test_version_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)

        event_for_get = {"pathParameters": {"dataset-id": "1234", "version-id": "1"}}

        response = version_handler.get_version(event_for_get, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
