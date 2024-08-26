import pytest
import json

from boto3.dynamodb.conditions import Key

from metadata.CommonRepository import ID_COLUMN
from metadata.edition.handler import (
    create_edition,
    delete_edition,
    get_edition,
    update_edition,
)
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

        response = create_edition(create_event, None)
        body = json.loads(response["body"])
        edition_id = body["Id"]

        expected_location = (
            f"/datasets/{dataset_id}/versions/6/editions/20190528T133700"
        )

        assert response["statusCode"] == 201
        assert response["headers"]["Location"] == expected_location
        assert edition_id == f"{dataset_id}/6/20190528T133700"

    def test_create_invalid_edition(self, metadata_table, auth_event, put_version):
        dataset_id, version = put_version
        invalid_edition = common_test_helper.raw_edition.copy()
        invalid_edition["edition"] = "2019-05-28T15:37:00"

        create_event = auth_event(invalid_edition, dataset=dataset_id, version=version)

        response = create_edition(create_event, None)
        message = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert message == {
            "message": "Validation error",
            "errors": ["edition: '2019-05-28T15:37:00' is not a 'date-time'"],
        }

    def test_create_duplicate_edition_should_fail(
        self, metadata_table, auth_event, put_version
    ):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )
        response = create_edition(create_event, None)
        assert response["statusCode"] == 201

        response = create_edition(create_event, None)
        assert response["statusCode"] == 409
        body = json.loads(response["body"])
        assert str.startswith(body[0]["message"], "Resource Conflict")

    def test_forbidden(self, metadata_table, event, put_version):
        dataset_id, version = put_version
        create_event = event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )

        response = create_edition(create_event, None)
        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {"message": f"You are not authorized to access dataset {dataset_id}"}
        ]

    def test_dataset_not_exist(self, metadata_table, auth_event):
        dataset_id = "some-dataset_id"
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version="1"
        )

        response = create_edition(create_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]

    def test_version_not_exist(self, metadata_table, auth_event, put_version):
        dataset_id, _ = put_version
        version_not_exist = "10"
        create_event = auth_event(
            common_test_helper.raw_edition,
            dataset=dataset_id,
            version=version_not_exist,
        )

        response = create_edition(create_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {
                "message": f"'Parent item with id {dataset_id}/{version_not_exist} does not exist'"
            }
        ]


class TestUpdateEdition:
    def test_update_edition(self, metadata_table, auth_event, put_version):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )
        body = json.loads(create_edition(create_event, None)["body"])
        edition_id = body["Id"].split("/")[-1]
        update_event = auth_event(
            common_test_helper.edition_updated,
            dataset=dataset_id,
            version=version,
            edition=edition_id,
        )

        response = update_edition(update_event, None)
        body = json.loads(response["body"])
        edition_id = body["Id"]
        assert response["statusCode"] == 200
        assert edition_id == "antall-besokende-pa-gjenbruksstasjoner/6/20190528T133700"

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(edition_id)
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
        body = json.loads(create_edition(create_event, None)["body"])
        edition = body["Id"].split("/")[-1]
        update_event = auth_event(
            common_test_helper.edition_updated,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        update_edition(update_event, None)

        edition_id = f"{dataset_id}/{version}/latest"
        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(edition_id)
        )
        edition_from_db = db_response["Items"][0]

        assert edition_from_db["latest"] == f"{dataset_id}/{version}/{edition}"

    def test_forbidden(self, metadata_table, auth_event, event, put_version):
        dataset_id, version = put_version
        create_event = auth_event(
            common_test_helper.raw_edition, dataset=dataset_id, version=version
        )
        body = json.loads(create_edition(create_event, None)["body"])

        edition_id = body["Id"].split("/")[-1]
        update_event = event(
            common_test_helper.edition_updated,
            dataset=dataset_id,
            version=version,
            edition=edition_id,
        )

        response = update_edition(update_event, None)
        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {"message": f"You are not authorized to access dataset {dataset_id}"}
        ]

    def test_dataset_not_exist(self, metadata_table, auth_event):
        dataset_id = "some-dataset_id"
        update_event = auth_event(
            common_test_helper.edition_updated,
            dataset=dataset_id,
            version="1",
            edition="20190603T092711",
        )

        response = update_edition(update_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]

    def test_version_not_exist(self, metadata_table, auth_event, put_version):
        dataset_id, version = put_version

        version_not_exist = "10"
        update_event = auth_event(
            common_test_helper.raw_edition,
            dataset=dataset_id,
            version=version_not_exist,
            edition="20190603T092711",
        )

        response = update_edition(update_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {
                "message": f"'Item with id {dataset_id}/{version_not_exist}/20190603T092711 does not exist'"
            }
        ]


class TestEdition:
    def test_edition_not_found(self, event):
        event_for_get = event({}, "1234", "1", "20190401T133700")
        response = get_edition(event_for_get, None)
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == {"message": "Edition not found."}


class TestDeleteEdition:
    def test_delete_ok(self, metadata_table, auth_event, put_edition):
        dataset_id, version, edition = put_edition

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq("/".join(put_edition))
        )
        assert db_response["Count"] == 1

        response = delete_edition(
            auth_event(dataset=dataset_id, version=version, edition=edition),
            None,
        )
        assert response["statusCode"] == 200

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq("/".join(put_edition))
        )
        assert db_response["Count"] == 0

    def test_forbidden(self, metadata_table, auth_event, event, put_edition):
        dataset_id, version, edition = put_edition

        response = delete_edition(
            event(dataset=dataset_id, version=version, edition=edition),
            None,
        )
        assert response["statusCode"] == 403

    def test_delete_not_found(self, auth_event):
        response = delete_edition(
            auth_event(dataset="foo", version="1", edition="bar"), None
        )
        assert response["statusCode"] == 404
