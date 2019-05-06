import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import edition_handler
import common_test_helper


class EditionTest(unittest.TestCase):
    @mock_dynamodb2
    def test_post_edition(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)
        _, _, response = common_test_helper.post_edition(
            common_test_helper.dataset_event,
            common_test_helper.version_event,
            common_test_helper.edition_event,
        )
        assert response[0]["statusCode"] == 200

    @mock_dynamodb2
    def test_update_edition(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        edition_table = common_test_helper.create_edition_table(dynamodb)

        response_from_dataset_post, response_from_version_post, response_from_edition_post = common_test_helper.post_edition(
            common_test_helper.dataset_event,
            common_test_helper.version_event,
            common_test_helper.edition_event,
        )
        response_from_edition_post = response_from_edition_post[0]

        assert response_from_edition_post["statusCode"] == 200

        edition_event_for_put = common_test_helper.edition_event_updated
        edition_event_for_put["pathParameters"] = {
            "version-id": common_test_helper.read_result_body(
                response_from_version_post
            ),
            "dataset-id": common_test_helper.read_result_body(
                response_from_dataset_post
            ),
            "edition-id": common_test_helper.read_result_body(
                response_from_edition_post
            ),
        }

        response = edition_handler.update_edition(edition_event_for_put, None)
        assert response["statusCode"] == 200

        db_response = edition_table.query(
            KeyConditionExpression=Key(table.EDITION_ID).eq(
                common_test_helper.read_result_body(response_from_edition_post)
            )
        )
        assert db_response["Items"][0]["description"] == "CHANGED"

    @mock_dynamodb2
    def test_get_all_editions(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)

        response_from_dataset_post, response_from_version_post, _ = common_test_helper.post_edition(
            common_test_helper.dataset_event,
            common_test_helper.version_event,
            common_test_helper.edition_event,
            common_test_helper.edition_event_updated,
        )

        path_parameter_for_get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.read_result_body(
                    response_from_dataset_post
                ),
                "version-id": common_test_helper.read_result_body(
                    response_from_version_post
                ),
            }
        }

        response = edition_handler.get_editions(path_parameter_for_get_event, None)

        response_body_as_json = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(response_body_as_json) == 2

    @mock_dynamodb2
    def test_get_one_edition(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)
        response_from_dataset_post, response_from_version_post, response_from_edition_post = common_test_helper.post_edition(
            common_test_helper.dataset_event,
            common_test_helper.version_event,
            common_test_helper.edition_event,
        )
        response_from_edition_post = response_from_edition_post[0]

        path_parameter_for_get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.read_result_body(
                    response_from_dataset_post
                ),
                "version-id": common_test_helper.read_result_body(
                    response_from_version_post
                ),
                "edition-id": common_test_helper.read_result_body(
                    response_from_edition_post
                ),
            }
        }

        response_from_get_as_json = json.loads(
            edition_handler.get_edition(path_parameter_for_get_event, None)["body"]
        )

        assert response_from_get_as_json[
            table.EDITION_ID
        ] == common_test_helper.read_result_body(response_from_edition_post)

    @mock_dynamodb2
    def test_edition_not_found(self):
        dynamodb = boto3.resource("dynamodb", "eu-west-1")
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)

        event_for_get = {
            "pathParameters": {
                "dataset-id": "1234",
                "version-id": "1",
                "edition-id": "20190401T133700",
            }
        }

        response = edition_handler.get_edition(event_for_get, None)

        assert response["statusCode"] == 404


if __name__ == "__main__":
    unittest.main()
