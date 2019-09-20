import pytest
import json

from boto3.dynamodb.conditions import Key

import metadata.common as table
import metadata.edition.handler as edition_handler
from tests import common_test_helper


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common_test_helper.create_metadata_table(dynamodb)


@pytest.fixture(autouse=True)
def edition_table(dynamodb):
    return common_test_helper.create_edition_table(dynamodb)


class TestCreateEdition:
    def test_create_edition(self, metadata_table, auth_event):
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        create_event = auth_event(
            common_test_helper.new_edition,
            dataset_id,
            common_test_helper.version_new_format["version"],
        )

        response = edition_handler.create_edition(create_event, None)
        edition_id = json.loads(response["body"])

        expected_location = (
            f"/datasets/{dataset_id}/versions/6/editions/20190528T133700"
        )

        assert response["statusCode"] == 200
        assert response["headers"]["Location"] == expected_location
        assert edition_id == f"{dataset_id}/6/20190528T133700"

    def test_create_invalid_edition(self, metadata_table, auth_event):
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]

        invalid_edition = common_test_helper.new_edition.copy()
        invalid_edition["edition"] = "2019-05-28T15:37:00"
        create_event = auth_event(
            invalid_edition,
            dataset_id,
            common_test_helper.version_new_format["version"],
        )

        response = edition_handler.create_edition(create_event, None)
        message = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert "is not a 'date-time'" in message

    def test_create_duplicate_edition_should_fail(self, metadata_table, auth_event):
        metadata_table.put_item(Item=common_test_helper.dataset_new_format)
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        create_event = auth_event(
            common_test_helper.new_edition,
            common_test_helper.dataset_new_format[table.ID_COLUMN],
            common_test_helper.version_new_format["version"],
        )
        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 200

        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 409
        assert str.startswith(json.loads(response["body"]), "Resource Conflict")

    def test_forbidden(self, metadata_table, event, auth_denied):
        metadata_table.put_item(Item=common_test_helper.version_new_format)

        dataset_id = common_test_helper.dataset_new_format[table.ID_COLUMN]
        create_event = event(
            common_test_helper.new_edition,
            dataset_id,
            common_test_helper.version_new_format["version"],
        )

        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 403


class TestUpdateEdition:
    def test_update_edition(self, metadata_table, auth_event):
        metadata_table.put_item(Item=common_test_helper.edition_new_format)
        update_event = auth_event(
            common_test_helper.edition_updated,
            "antall-besokende-pa-gjenbruksstasjoner",
            "6",
            "20190528T133700",
        )

        response = edition_handler.update_edition(update_event, None)
        edition_id = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert edition_id == f"antall-besokende-pa-gjenbruksstasjoner/6/20190528T133700"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(edition_id)
        )
        edition_from_db = db_response["Items"][0]

        assert edition_from_db["edition"] == "2019-05-28T15:37:00+02:00"
        assert edition_from_db["description"] == "CHANGED"

    def test_update_edition_latest_is_updated(self, metadata_table, auth_event):
        dataset_id = "antall-besokende-pa-gjenbruksstasjoner"
        version_name = "6"
        edition = "20190528T133700"

        create_event = auth_event(
            common_test_helper.new_edition, dataset_id, version_name
        )
        # Insert parent first:
        metadata_table.put_item(Item=common_test_helper.version_new_format)
        edition_handler.create_edition(create_event, None)

        update_event = auth_event(
            common_test_helper.edition_updated, dataset_id, version_name, edition
        )

        edition_handler.update_edition(update_event, None)

        edition_id = f"antall-besokende-pa-gjenbruksstasjoner/6/latest"
        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(edition_id)
        )
        edition_from_db = db_response["Items"][0]

        assert (
            edition_from_db["latest"]
            == "antall-besokende-pa-gjenbruksstasjoner/6/20190528T133700"
        )

    def test_forbidden(self, metadata_table, event, auth_denied):
        metadata_table.put_item(Item=common_test_helper.edition_new_format)
        update_event = event(
            common_test_helper.edition_updated,
            "antall-besokende-pa-gjenbruksstasjoner",
            "6",
            "20190528T133700",
        )

        response = edition_handler.update_edition(update_event, None)
        assert response["statusCode"] == 403


class TestEdition:
    def test_get_all_editions_from_new_table_if_present(
        self, metadata_table, edition_table, event
    ):
        edition_table.put_item(Item=common_test_helper.edition)
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        dataset_id = common_test_helper.dataset[table.DATASET_ID]
        version = common_test_helper.version["version"]
        get_all_event = event({}, dataset_id, version)

        response = edition_handler.get_editions(get_all_event, None)

        editions_from_db = json.loads(response["body"])
        self_url = editions_from_db[0].pop("_links")["self"]["href"]

        assert response["statusCode"] == 200
        assert len(editions_from_db) == 1
        assert editions_from_db[0] == common_test_helper.edition_new_format
        assert (
            self_url
            == f"/datasets/{dataset_id}/versions/{version}/editions/20190528T133700"
        )

    def test_get_all_editions_legacy(self, edition_table, event):
        edition = common_test_helper.edition
        edition_table.put_item(Item=edition)
        edition_table.put_item(Item=common_test_helper.edition_updated)

        get_all_event = event({}, edition[table.DATASET_ID], edition[table.VERSION_ID])
        response = edition_handler.get_editions(get_all_event, None)

        editions_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(editions_from_db) == 2

    def test_should_fetch_edition_from_new_table_if_present(
        self, metadata_table, edition_table, event
    ):
        edition_table.put_item(Item=common_test_helper.edition)
        metadata_table.put_item(Item=common_test_helper.edition_new_format)

        dataset_id = common_test_helper.dataset[table.DATASET_ID]
        version = common_test_helper.version["version"]
        get_event = event({}, dataset_id, version, "20190528T133700")

        response = edition_handler.get_edition(get_event, None)
        edition_from_db = json.loads(response["body"])
        self_url = edition_from_db.pop("_links")["self"]["href"]

        assert response["statusCode"] == 200
        assert edition_from_db == common_test_helper.edition_new_format
        assert (
            self_url
            == f"/datasets/{dataset_id}/versions/{version}/editions/20190528T133700"
        )

    def test_get_one_edition_legacy(self, edition_table, event):
        edition = common_test_helper.edition
        edition_id = edition[table.EDITION_ID]

        edition_table.put_item(Item=edition)

        get_event = event(
            {}, edition[table.DATASET_ID], edition[table.VERSION_ID], edition_id
        )
        response = edition_handler.get_edition(get_event, None)
        edition_from_db = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert edition_from_db == edition

    def test_edition_not_found(self, event):
        event_for_get = event({}, "1234", "1", "20190401T133700")
        response = edition_handler.get_edition(event_for_get, None)
        assert response["statusCode"] == 404
