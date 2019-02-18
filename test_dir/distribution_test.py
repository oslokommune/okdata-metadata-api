import json
import unittest

import boto3
from boto3.dynamodb.conditions import Key
from moto import mock_dynamodb2

import common as table
import distribution_handler
from test_dir import common_test_helper


class DistributionTest(unittest.TestCase):
    @mock_dynamodb2
    def test_post_distribution(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)
        common_test_helper.create_distribution_table(dynamodb)

        d_res, v_res, e_res, response = common_test_helper.post_distribution(common_test_helper.dataset_event,
                                                                             common_test_helper.version_event,
                                                                             common_test_helper.edition_event,
                                                                             common_test_helper.distribution_event)

        assert response[0]["statusCode"] == 200

        distribution_event = common_test_helper.distribution_event
        distribution_event["pathParameters"] = {
            "dataset-id": common_test_helper.read_result_body(d_res),
            "version-id": common_test_helper.read_result_body(v_res),
            "edition-id": "LOLOLFEIL"
        }

        response = distribution_handler.post_distribution(distribution_event, None)
        assert response["statusCode"] == 404

    @mock_dynamodb2
    def test_update_distribution(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)
        distribution_table = common_test_helper.create_distribution_table(dynamodb)

        d_res, v_res, e_res, response_from_post = common_test_helper.post_distribution(common_test_helper.dataset_event,
                                                                                       common_test_helper.version_event,
                                                                                       common_test_helper.edition_event,
                                                                                       common_test_helper.distribution_event)
        response_from_post = response_from_post[0]
        assert response_from_post["statusCode"] == 200

        distribution_event_for_put = common_test_helper.distribution_event_updated
        distribution_event_for_put["pathParameters"] = {
            "dataset-id": common_test_helper.read_result_body(d_res),
            "version-id": common_test_helper.read_result_body(v_res),
            "edition-id": common_test_helper.read_result_body(e_res),
            "distribution-id": common_test_helper.read_result_body(response_from_post)
        }

        response_from_post = distribution_handler.update_distribution(distribution_event_for_put, None)
        response_from_distribution_put = common_test_helper.read_result_body(response_from_post)

        db_response = distribution_table.query(
            KeyConditionExpression=Key(table.DISTRIBUTION_ID).eq(response_from_distribution_put)
        )
        assert db_response["Items"][0]["filename"] == "UPDATED.csv"

    @mock_dynamodb2
    def test_get_all_distributions(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)
        common_test_helper.create_distribution_table(dynamodb)

        d_res, v_res, e_res, response_from_post = common_test_helper.post_distribution(common_test_helper.dataset_event,
                                                                                       common_test_helper.version_event,
                                                                                       common_test_helper.edition_event,
                                                                                       common_test_helper.distribution_event,
                                                                                       common_test_helper.distribution_event_updated)

        path_parameter_for_get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.read_result_body(d_res),
                "version-id": common_test_helper.read_result_body(v_res),
                "edition-id": common_test_helper.read_result_body(e_res)
            }}

        response = distribution_handler.get_distributions(path_parameter_for_get_event, None)

        response_body_as_json = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(response_body_as_json) == 2

    @mock_dynamodb2
    def test_get_one_distribution(self):
        dynamodb = boto3.resource('dynamodb', 'eu-west-1')
        common_test_helper.create_dataset_table(dynamodb)
        common_test_helper.create_version_table(dynamodb)
        common_test_helper.create_edition_table(dynamodb)
        common_test_helper.create_distribution_table(dynamodb)

        d_res, v_res, e_res, response_from_post = common_test_helper.post_distribution(common_test_helper.dataset_event,
                                                                                       common_test_helper.version_event,
                                                                                       common_test_helper.edition_event,
                                                                                       common_test_helper.distribution_event,
                                                                                       common_test_helper.distribution_event_updated)
        response_from_post = response_from_post[0]

        path_parameter_for_get_event = {
            "pathParameters": {
                "dataset-id": common_test_helper.read_result_body(d_res),
                "version-id": common_test_helper.read_result_body(v_res),
                "edition-id": common_test_helper.read_result_body(e_res),
                "distribution-id": common_test_helper.read_result_body(response_from_post)
            }}

        get_all_response = distribution_handler.get_distribution(path_parameter_for_get_event, None)
        assert get_all_response["statusCode"] == 200

        response_from_get_as_json = common_test_helper.read_result_body(get_all_response)

        assert len(response_from_get_as_json) == 1
        assert response_from_get_as_json[0][table.DISTRIBUTION_ID] == common_test_helper.read_result_body(response_from_post)


if __name__ == '__main__':
    unittest.main()
