import pytest
import json
import re

from boto3.dynamodb.conditions import Key

import metadata.common as table
import metadata.distribution.handler as distribution_handler
from tests import common_test_helper


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common_test_helper.create_metadata_table(dynamodb)


class TestCreateDistribution:
    def test_create_distribution(self, metadata_table, auth_event, put_edition):
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            common_test_helper.raw_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = distribution_handler.create_distribution(create_event, None)
        body = json.loads(response["body"])
        distribution_id = body["Id"]
        id_regex = f"{dataset_id}/{version}/{edition}/[0-9a-f-]+"
        location_regex = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/[0-9a-f-]+"

        assert response["statusCode"] == 201
        assert re.fullmatch(id_regex, distribution_id)
        assert re.fullmatch(location_regex, response["headers"]["Location"])

        # Creating distribution for non-existing edition should fail
        bad_create_event = create_event
        bad_create_event["pathParameters"]["edition"] = "LOLOLFEIL"

        response = distribution_handler.create_distribution(bad_create_event, None)
        assert response["statusCode"] == 400

    def test_forbidden(self, metadata_table, auth_event, put_edition, auth_denied):
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            common_test_helper.raw_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = distribution_handler.create_distribution(create_event, None)
        assert response["statusCode"] == 403


class TestUpdateDistribution:
    def test_update_distribution(self, metadata_table, auth_event, put_edition):
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            common_test_helper.raw_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = distribution_handler.create_distribution(create_event, None)
        body = json.loads(response["body"])
        distribution_id = body["Id"]

        update_event = auth_event(
            common_test_helper.distribution_updated,
            dataset=dataset_id,
            version=version,
            edition=edition,
            distribution=distribution_id.split("/")[-1],
        )

        response = distribution_handler.update_distribution(update_event, None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Id"] == distribution_id

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(distribution_id)
        )
        assert db_response["Items"][0]["filename"] == "UPDATED.csv"

    def test_forbidden(self, metadata_table, auth_event, auth_denied, put_edition):
        # auth_denied will return Forbidden, and then no edition will be created....
        edition = "my-edition"
        dataset_id, version, _ = put_edition

        create_event = auth_event(
            common_test_helper.raw_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = distribution_handler.create_distribution(create_event, None)
        assert response["statusCode"] == 403


class TestDistribution:
    def test_distribution_not_found(self, event):
        event_for_get = event(
            {}, "1234", "1", "20190401T133700", "6f563c62-8fe4-4591-a999-5fbf0798e268"
        )
        response = distribution_handler.get_distribution(event_for_get, None)
        assert response["statusCode"] == 404
