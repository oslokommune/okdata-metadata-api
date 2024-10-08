import pytest
import json
import re
from unittest.mock import patch

from boto3.dynamodb.conditions import Key

from metadata.CommonRepository import ID_COLUMN
from metadata.distribution.handler import (
    create_distribution,
    delete_distribution,
    get_distribution,
    get_distributions,
    update_distribution,
)
from metadata.distribution.repository import DistributionRepository
from tests import common_test_helper


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common_test_helper.create_metadata_table(dynamodb)


class TestCreateDistribution:
    def test_create_distribution(self, metadata_table, auth_event, put_edition):
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            common_test_helper.raw_file_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(create_event, None)
        body = json.loads(response["body"])

        distribution_id = body["Id"]
        id_regex = f"{dataset_id}/{version}/{edition}/[0-9a-f-]+"
        location_regex = f"/datasets/{dataset_id}/versions/{version}/editions/{edition}/distributions/[0-9a-f-]+"

        assert response["statusCode"] == 201
        assert body["distribution_type"] == "file"
        assert body["content_type"] == "text/csv"
        assert body["filenames"] == ["BOOOM.csv"]
        assert re.fullmatch(id_regex, distribution_id)
        assert re.fullmatch(location_regex, response["headers"]["Location"])

    def test_create_distribution_set_default_type(
        self, metadata_table, auth_event, put_edition
    ):
        content = common_test_helper.raw_file_distribution.copy()
        content.pop("distribution_type")
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 201
        assert body["distribution_type"] == "file"

    def test_create_api_distribution(self, metadata_table, auth_event, put_edition):
        content = common_test_helper.raw_api_distribution.copy()
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 201
        assert body["distribution_type"] == "api"

    def test_create_distribution_non_existing_edition(
        self, metadata_table, auth_event, put_edition
    ):
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            common_test_helper.raw_file_distribution,
            dataset=dataset_id,
            version=version,
            edition="LOLOLFEIL",
        )

        context = common_test_helper.Context("1234")
        response = create_distribution(bad_create_event, context)

        assert response["statusCode"] == 500
        assert json.loads(response["body"]) == {
            "message": "Error creating distribution. RequestId: 1234"
        }

    def test_create_distribution_wrong_type(
        self, metadata_table, auth_event, put_edition
    ):
        bad_content = common_test_helper.raw_file_distribution.copy()
        bad_content["distribution_type"] = "foo"
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            bad_content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(bad_create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert body["message"] == "Validation error"
        assert body["errors"] == [
            "distribution_type: 'foo' is not one of ['file', 'api']"
        ]

    def test_create_file_distribution_missing_filenames(
        self, metadata_table, auth_event, put_edition
    ):
        bad_content = common_test_helper.raw_file_distribution.copy()
        del bad_content["filename"]
        del bad_content["filenames"]
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            bad_content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(bad_create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert (
            body[0]["message"]
            == "Missing 'filenames', required when 'distribution_type' is 'file'."
        )

    def test_create_distribution_wrong_type_for_filenames(
        self, metadata_table, auth_event, put_edition
    ):
        bad_content = common_test_helper.raw_api_distribution.copy()
        bad_content["filenames"] = ["foo.json"]
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            bad_content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(bad_create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert (
            body[0]["message"]
            == "'filenames' is only supported when 'distribution_type' is 'file', got 'api'."
        )

    def test_create_distribution_missing_api_url(
        self, metadata_table, auth_event, put_edition
    ):
        bad_content = common_test_helper.raw_api_distribution.copy()
        del bad_content["api_url"]
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            bad_content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(bad_create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert (
            body[0]["message"]
            == "Missing 'api_url', required when 'distribution_type' is 'api'."
        )

    def test_create_distribution_wrong_type_for_api_url(
        self, metadata_table, auth_event, put_edition
    ):
        bad_content = common_test_helper.raw_file_distribution.copy()
        bad_content["api_url"] = "https://example.org"
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            bad_content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(bad_create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert (
            body[0]["message"]
            == "'api_url' is only supported when 'distribution_type' is 'api', got 'file'."
        )

    def test_create_distribution_wrong_type_for_api_id(
        self, metadata_table, auth_event, put_edition
    ):
        bad_content = common_test_helper.raw_file_distribution.copy()
        bad_content["api_id"] = "foo:123"
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            bad_content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(bad_create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert (
            body[0]["message"]
            == "'api_id' is only supported when 'distribution_type' is 'api', got 'file'."
        )

    def test_create_distribution_unknown_api_namespace(
        self, metadata_table, auth_event, put_edition
    ):
        bad_content = common_test_helper.raw_api_distribution.copy()
        bad_content["api_id"] = "foo:123"
        dataset_id, version, edition = put_edition
        bad_create_event = auth_event(
            bad_content,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )

        response = create_distribution(bad_create_event, None)
        body = json.loads(response["body"])

        assert response["statusCode"] == 400
        assert (
            body[0]["message"]
            == "API namespace must be one of ['okdata-api-catalog'], was 'foo'."
        )

    def test_forbidden(self, metadata_table, event, put_edition):
        dataset_id, version, edition = put_edition
        create_event = event(
            common_test_helper.raw_file_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = create_distribution(create_event, None)
        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {"message": f"You are not authorized to access dataset {dataset_id}"}
        ]

    def test_dataset_not_exist(self, metadata_table, auth_event):
        dataset_id = "some-dataset_id"
        create_event = auth_event(
            common_test_helper.raw_file_distribution,
            dataset=dataset_id,
            version="1",
            edition="20190603T092711",
        )

        response = create_distribution(create_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]


class TestUpdateDistribution:
    def test_update_distribution(self, metadata_table, auth_event, put_edition):
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            common_test_helper.raw_file_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = create_distribution(create_event, None)
        body = json.loads(response["body"])
        distribution_id = body["Id"]

        update_event = auth_event(
            common_test_helper.distribution_updated,
            dataset=dataset_id,
            version=version,
            edition=edition,
            distribution=distribution_id.split("/")[-1],
        )

        response = update_distribution(update_event, None)
        assert response["statusCode"] == 200
        body = json.loads(response["body"])
        assert body["Id"] == distribution_id

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(distribution_id)
        )
        item = db_response["Items"][0]

        assert item["filename"] == "UPDATED.csv"

    def test_forbidden(self, metadata_table, event, put_edition):
        # auth_denied will return Forbidden, and then no edition will be created....
        edition = "my-edition"
        dataset_id, version, _ = put_edition

        update_event = event(
            common_test_helper.distribution_updated,
            dataset=dataset_id,
            version=version,
            edition=edition,
            distribution="52ee4425-3a3c-4a9f-b599-869de889d30c",
        )

        response = update_distribution(update_event, None)
        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {"message": f"You are not authorized to access dataset {dataset_id}"}
        ]

    def test_dataset_not_exist(self, metadata_table, auth_event):
        dataset_id = "some-dataset_id"
        update_event = auth_event(
            common_test_helper.distribution_updated,
            dataset=dataset_id,
            version="1",
            edition="20190603T092711",
            distribution="52ee4425-3a3c-4a9f-b599-869de889d30c",
        )

        response = update_distribution(update_event, None)

        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]


class TestGetDistribution:
    def test_distribution_not_found(self, event):
        event_for_get = event(
            {}, "1234", "1", "20190401T133700", "6f563c62-8fe4-4591-a999-5fbf0798e268"
        )
        response = get_distribution(event_for_get, None)
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == {"message": "Distribution not found."}

    def test_get_distribution(self, event, metadata_table):
        distribution_id = "1234/1/20190401T133700/6f563c62-8fe4-4591-a999-5fbf0798e268"
        metadata_table.put_item(
            Item={
                "Id": distribution_id,
                "Type": "Distribution",
                "distribution_type": "file",
                "filenames": ["file.csv"],
            }
        )

        event_for_get = event(
            {}, "1234", "1", "20190401T133700", "6f563c62-8fe4-4591-a999-5fbf0798e268"
        )
        response = get_distribution(event_for_get, None)

        assert response["statusCode"] == 200

        body = json.loads(response["body"])
        assert body["Id"] == distribution_id
        assert body["Type"] == "Distribution"
        assert body["distribution_type"] == "file"
        assert body["content_type"] == "text/csv"
        assert body["filenames"] == ["file.csv"]


class TestGetDistributions:
    def test_no_distributions(self, event):
        event_for_get = event({}, "1234", "1", "20190401T133700")
        response = get_distributions(event_for_get, None)

        assert response["statusCode"] == 200
        assert json.loads(response["body"]) == []

    def test_get_distributions(self, event, metadata_table):
        distribution_id = "1234/1/20190401T133700/6f563c62-8fe4-4591-a999-5fbf0798e268"
        metadata_table.put_item(
            Item={
                "Id": distribution_id,
                "Type": "Distribution",
                "distribution_type": "file",
                "filenames": ["file.csv"],
            }
        )

        event_for_get = event({}, "1234", "1", "20190401T133700")
        response = get_distributions(event_for_get, None)

        assert response["statusCode"] == 200

        body = json.loads(response["body"])
        assert len(body) == 1

        distribution = body[0]
        assert distribution["Id"] == distribution_id
        assert distribution["Type"] == "Distribution"
        assert distribution["distribution_type"] == "file"
        assert distribution["content_type"] == "text/csv"
        assert distribution["filenames"] == ["file.csv"]


class TestDeleteDistribution:
    def test_delete_ok(self, metadata_table, auth_event, put_edition):
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            common_test_helper.raw_file_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = create_distribution(create_event, None)
        distribution_id = json.loads(response["body"])["Id"]

        with patch("metadata.distribution.handler.DistributionRepository._delete_data"):
            response = delete_distribution(
                auth_event(
                    dataset=dataset_id,
                    version=version,
                    edition=edition,
                    distribution=distribution_id.split("/")[-1],
                ),
                None,
            )
        assert response["statusCode"] == 200

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(distribution_id)
        )
        assert db_response["Count"] == 0

    def test_forbidden(self, metadata_table, auth_event, event, put_edition):
        dataset_id, version, edition = put_edition
        create_event = auth_event(
            common_test_helper.raw_file_distribution,
            dataset=dataset_id,
            version=version,
            edition=edition,
        )
        response = create_distribution(create_event, None)
        distribution_id = json.loads(response["body"])["Id"]

        response = delete_distribution(
            event(
                dataset=dataset_id,
                version=version,
                edition=edition,
                distribution=distribution_id.split("/")[-1],
            ),
            None,
        )
        assert response["statusCode"] == 403

    def test_delete_not_found(self, auth_event):
        response = delete_distribution(
            auth_event(dataset="foo", version="1", edition="bar", distribution="baz"),
            None,
        )
        assert response["statusCode"] == 404


class TestDeriveContentType:
    def test_empty_item(self):
        item = {}
        DistributionRepository._derive_content_type(item)
        assert "content_type" not in item

    def test_wrong_distribution_type(self):
        item = {"filenames": ["foo.csv"], "distribution_type": "api"}
        DistributionRepository._derive_content_type(item)
        assert "content_type" not in item

    def test_override_content_type(self):
        item = {
            "filenames": ["foo.csv"],
            "distribution_type": "file",
            "content_type": "definitely_not_csv",
        }
        DistributionRepository._derive_content_type(item)
        assert item["content_type"] == "definitely_not_csv"

    def test_based_on_filename(self):
        item = {"filename": "foo.csv", "distribution_type": "file"}
        DistributionRepository._derive_content_type(item)
        assert item["content_type"] == "text/csv"

    def test_based_on_filenames(self):
        item = {"filenames": ["foo.csv"], "distribution_type": "file"}
        DistributionRepository._derive_content_type(item)
        assert item["content_type"] == "text/csv"

    def test_filenames_preferred(self):
        item = {
            "filename": "foo.parquet",
            "filenames": ["foo.csv"],
            "distribution_type": "file",
        }
        DistributionRepository._derive_content_type(item)
        assert item["content_type"] == "text/csv"

    def test_missing_filename(self):
        item = {"distribution_type": "file"}
        DistributionRepository._derive_content_type(item)
        assert "content_type" not in item

    @pytest.mark.parametrize(
        "ext,mime_type",
        [
            ("csv", "text/csv"),
            ("CSV", "text/csv"),
            ("json", "application/json"),
            ("parq", "application/parquet"),
            ("PARQ", "application/parquet"),
            ("parq.gz", "application/parquet"),
            ("parquet", "application/parquet"),
            ("parquet.gz", "application/parquet"),
            ("xls", "application/vnd.ms-excel"),
            (
                "xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
        ],
    )
    def test_important_types(self, ext, mime_type):
        item = {"filenames": [f"foo.{ext}"], "distribution_type": "file"}
        DistributionRepository._derive_content_type(item)
        assert item["content_type"] == mime_type


class TestDeleteData:
    def test_delete_data(self, s3_client, s3_bucket, metadata_table):
        key = "raw/red/foo/version=1/edition=latest/bar.txt"

        metadata_table.put_item(
            Item={"Id": "foo", "Type": "Dataset", "accessRights": "non-public"}
        )
        metadata_table.put_item(Item={"Id": "foo/1", "Type": "Version"})
        metadata_table.put_item(Item={"Id": "foo/1/latest", "Type": "Edition"})
        metadata_table.put_item(
            Item={
                "Id": "foo/1/latest/bar",
                "Type": "Distribution",
                "filenames": "bar.txt",
            }
        )
        s3_client.put_object(Bucket=s3_bucket, Key=key, Body="baz")

        objs = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=key)
        assert objs["KeyCount"] == 1

        DistributionRepository()._delete_data("foo", "1", "latest", "bar")

        objs = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=key)
        assert objs["KeyCount"] == 0
