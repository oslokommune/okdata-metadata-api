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


@pytest.fixture(autouse=True)
def distribution_table(dynamodb):
    return common_test_helper.create_distribution_table(dynamodb)


class TestCreateDistribution:
    def test_create_distribution(self, metadata_table, auth_event):
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]

        create_event = auth_event(
            common_test_helper.new_distribution,
            dataset_id,
            common_test_helper.version["version"],
            "20190528T133700",
        )
        response = distribution_handler.create_distribution(create_event, None)
        distribution_id = json.loads(response["body"])

        id_regex = f"{dataset_id}/6/20190528T133700/[0-9a-f-]+"
        location_regex = f"/datasets/{dataset_id}/versions/6/editions/20190528T133700/distributions/[0-9a-f-]+"

        assert response["statusCode"] == 200
        assert re.fullmatch(id_regex, distribution_id)
        assert re.fullmatch(location_regex, response["headers"]["Location"])

        # Creating distribution for non-existing edition should fail
        bad_create_event = create_event
        bad_create_event["pathParameters"]["edition"] = "LOLOLFEIL"

        response = distribution_handler.create_distribution(bad_create_event, None)
        assert response["statusCode"] == 400

    def test_forbidden(self, metadata_table, event, auth_denied):
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]

        create_event = event(
            common_test_helper.new_distribution,
            dataset_id,
            common_test_helper.version["version"],
            "20190528T133700",
        )
        response = distribution_handler.create_distribution(create_event, None)
        assert response["statusCode"] == 403


class TestUpdateDistribution:
    def test_update_distribution(self, metadata_table, auth_event):
        metadata_table.put_item(Item=common_test_helper.distribution_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        distribution_id = (
            f"{dataset_id}/6/20190528T133700/e80b5f2c-67f0-4a50-a6d9-b6a565ef2401"
        )
        update_event = auth_event(
            common_test_helper.distribution_updated,
            dataset_id,
            common_test_helper.version["version"],
            "20190528T133700",
            "e80b5f2c-67f0-4a50-a6d9-b6a565ef2401",
        )

        response = distribution_handler.update_distribution(update_event, None)
        assert response["statusCode"] == 200
        assert json.loads(response["body"]) == distribution_id

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.DISTRIBUTION_ID).eq(distribution_id)
        )
        assert db_response["Items"][0]["filename"] == "UPDATED.csv"

    def test_forbidden(self, metadata_table, event, auth_denied):
        metadata_table.put_item(Item=common_test_helper.distribution_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        update_event = event(
            common_test_helper.distribution_updated,
            dataset_id,
            common_test_helper.version["version"],
            "20190528T133700",
            "e80b5f2c-67f0-4a50-a6d9-b6a565ef2401",
        )

        response = distribution_handler.update_distribution(update_event, None)
        assert response["statusCode"] == 403


class TestDistribution:
    def test_get_all_distributions_new_table(
        self, metadata_table, distribution_table, event
    ):
        distribution_table.put_item(Item=common_test_helper.distribution)
        metadata_table.put_item(Item=common_test_helper.distribution_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version = common_test_helper.version["version"]
        get_all_event = event({}, dataset_id, version, "20190528T133700")

        response = distribution_handler.get_distributions(get_all_event, None)
        distributions_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(distributions_from_db) == 1

        self_url = distributions_from_db[0].pop("_links")["self"]["href"]
        assert distributions_from_db[0] == common_test_helper.distribution_new_format
        assert (
            self_url
            == f"/datasets/{dataset_id}/versions/{version}/editions/20190528T133700/distributions/e80b5f2c-67f0-4a50-a6d9-b6a565ef2401"
        )

    def test_get_all_distributions_legacy(self, distribution_table, event):
        distribution_table.put_item(Item=common_test_helper.distribution)
        distribution_table.put_item(Item=common_test_helper.distribution_updated)

        get_all_event = event(
            {},
            common_test_helper.distribution[table.DATASET_ID],
            common_test_helper.distribution[table.VERSION_ID],
            common_test_helper.distribution[table.EDITION_ID],
        )

        response = distribution_handler.get_distributions(get_all_event, None)
        distributions_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(distributions_from_db) == 2

    def test_should_fetch_distribution_from_new_table_if_present(
        self, metadata_table, distribution_table, event
    ):
        distribution_table.put_item(Item=common_test_helper.distribution)
        metadata_table.put_item(Item=common_test_helper.distribution_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        version = common_test_helper.version["version"]
        distribution_id = common_test_helper.distribution_new_format[table.ID_COLUMN]
        get_event = event(
            {},
            dataset_id,
            version,
            "20190528T133700",
            "e80b5f2c-67f0-4a50-a6d9-b6a565ef2401",
        )

        response = distribution_handler.get_distribution(get_event, None)
        distribution_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert distribution_from_db[table.ID_COLUMN] == distribution_id

        self_url = distribution_from_db.pop("_links")["self"]["href"]
        assert (
            self_url
            == f"/datasets/{dataset_id}/versions/{version}/editions/20190528T133700/distributions/e80b5f2c-67f0-4a50-a6d9-b6a565ef2401"
        )

    def test_get_one_distribution_legacy(self, distribution_table, event):
        distribution_table.put_item(Item=common_test_helper.distribution)
        distribution_table.put_item(Item=common_test_helper.distribution_updated)

        distribution_id = common_test_helper.distribution[table.DISTRIBUTION_ID]
        get_event = event(
            {},
            common_test_helper.distribution[table.DATASET_ID],
            common_test_helper.distribution[table.VERSION_ID],
            common_test_helper.distribution[table.EDITION_ID],
            distribution_id,
        )

        response = distribution_handler.get_distribution(get_event, None)
        distribution_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert distribution_from_db[table.DISTRIBUTION_ID] == distribution_id

    def test_distribution_not_found(self, event):
        event_for_get = event(
            {}, "1234", "1", "20190401T133700", "6f563c62-8fe4-4591-a999-5fbf0798e268"
        )
        response = distribution_handler.get_distribution(event_for_get, None)
        assert response["statusCode"] == 404
