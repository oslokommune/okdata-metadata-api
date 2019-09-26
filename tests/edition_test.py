import pytest
import json

from boto3.dynamodb.conditions import Key

import metadata.common as table
import metadata.edition.handler as edition_handler
from tests import common_test_helper


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common_test_helper.create_metadata_table(dynamodb)


class TestCreateEdition:
    def test_create_edition(self, metadata_table, auth_event, put_version):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )

        response = edition_handler.create_edition(create_event, None)
        edition_id = json.loads(response["body"])

        expected_location = (
            f"/datasets/{dataset_id}/versions/6/editions/20190528T133700"
        )

        assert response["statusCode"] == 200
        assert response["headers"]["Location"] == expected_location
        assert edition_id == f"{dataset_id}/6/20190528T133700"

    def test_create_invalid_edition(self, metadata_table, auth_event, put_version):
        dataset_id, version = put_version
        invalid_edition = common_test_helper.raw_edition.copy()
        invalid_edition["edition"] = "2019-05-28T15:37:00"

        create_event = auth_event(invalid_edition, dataset=dataset_id, version=version)

        response = edition_handler.create_edition(create_event, None)
        message = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert "is not a 'date-time'" in message

    def test_create_duplicate_edition_should_fail(
        self, metadata_table, auth_event, put_version
    ):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )
        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 200

        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 409
        assert str.startswith(json.loads(response["body"]), "Resource Conflict")

    def test_forbidden(self, metadata_table, auth_event, put_version, auth_denied):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )

        response = edition_handler.create_edition(create_event, None)
        assert response["statusCode"] == 403


class TestUpdateEdition:
    def test_update_edition(self, metadata_table, auth_event, put_version):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )
        edition_id = json.loads(
            edition_handler.create_edition(create_event, None)["body"]
        ).split("/")[-1]
        update_event = auth_event(
            common_test_helper.edition_updated,
            dataset=dataset_id,
            version=version,
            edition=edition_id,
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

    def test_update_edition_latest_is_updated(
        self, metadata_table, auth_event, put_version
    ):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )
        edition = json.loads(
            edition_handler.create_edition(create_event, None)["body"]
        ).split("/")[-1]
        update_event = auth_event(
            common_test_helper.edition_updated,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        edition_handler.update_edition(update_event, None)

        edition_id = f"{dataset_id}/{version}/latest"
        db_response = metadata_table.query(
            KeyConditionExpression=Key(table.ID_COLUMN).eq(edition_id)
        )
        edition_from_db = db_response["Items"][0]

        assert edition_from_db["latest"] == f"{dataset_id}/{version}/{edition}"

    def test_forbidden(self, metadata_table, auth_event, put_version, auth_denied):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )
        edition_id = json.loads(
            edition_handler.create_edition(create_event, None)["body"]
        ).split("/")[-1]
        update_event = auth_event(
            common_test_helper.edition_updated,
            dataset=dataset_id,
            version=version,
            edition=edition_id,
        )

        response = edition_handler.update_edition(update_event, None)
        assert response["statusCode"] == 403


class TestEdition:
    def test_edition_not_found(self, event):
        event_for_get = event({}, "1234", "1", "20190401T133700")
        response = edition_handler.get_edition(event_for_get, None)
        assert response["statusCode"] == 404
