import pytest
import json
from decimal import Decimal

from boto3.dynamodb.conditions import Key

from metadata.CommonRepository import ID_COLUMN, TYPE_COLUMN
import metadata.dataset.repository as dataset_repository
import tests.common_test_helper as common


@pytest.fixture(autouse=True)
def metadata_table(dynamodb):
    return common.create_metadata_table(dynamodb)


class TestCreateDataset:
    def test_create(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        create_event = auth_event(common.raw_dataset.copy())
        response = dataset_handler.create_dataset(create_event, None)
        body = json.loads(response["body"])
        dataset_id = body["Id"]
        assert response["statusCode"] == 201
        assert response["headers"]["Location"] == f"/datasets/{dataset_id}"
        assert dataset_id == "antall-besokende-pa-gjenbruksstasjoner"
        assert body["source"] == {"type": "file"}

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item[ID_COLUMN] == dataset_id
        assert item[TYPE_COLUMN] == "Dataset"
        assert item["title"] == "Antall besøkende på gjenbruksstasjoner"
        assert item["state"] == "active"

        # Check that we create the initial version

        version_id = f"{dataset_id}/1"
        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(version_id)
        )
        item = db_response["Items"][0]
        assert item[ID_COLUMN] == version_id
        assert item[TYPE_COLUMN] == "Version"
        assert item["version"] == "1"

        # Check that we create the "latest" version alias

        latest_id = f"{dataset_id}/latest"
        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(latest_id)
        )
        item = db_response["Items"][0]
        assert item[ID_COLUMN] == latest_id
        assert item[TYPE_COLUMN] == "Version"
        assert item["version"] == "1"
        assert item["latest"] == version_id

    def test_create_invalid(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        invalid_dataset = common.raw_dataset.copy()
        invalid_dataset["accessRights"] = "foo"
        create_event = auth_event(invalid_dataset)
        response = dataset_handler.create_dataset(create_event, None)
        error_message = json.loads(response["body"])
        assert response["statusCode"] == 400
        assert error_message == {
            "message": "Validation error",
            "errors": [
                "accessRights: 'foo' is not one of ['non-public', 'public', 'restricted']"
            ],
        }

    def test_create_with_parent(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        parent = common.raw_dataset.copy()
        parent["source"] = {"type": "none"}
        res = dataset_handler.create_dataset(auth_event(parent), None)
        parent_id = json.loads(res["body"])["Id"]

        child = common.raw_dataset.copy()
        child["parent_id"] = parent_id
        res = dataset_handler.create_dataset(auth_event(child), None)

        assert res["statusCode"] == 201

        child_id = json.loads(res["body"])["Id"]

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(child_id)
        )
        item = db_response["Items"][0]

        assert item["title"] == "Antall besøkende på gjenbruksstasjoner"
        assert item["parent_id"] == parent_id

    def test_create_with_missing_parent(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        child = common.raw_dataset.copy()
        child["parent_id"] = "non-existing"

        res = dataset_handler.create_dataset(auth_event(child), None)
        assert res["statusCode"] == 400
        assert (
            json.loads(res["body"])[0]["message"]
            == "Parent dataset 'non-existing' doesn't exist."
        )

    def test_create_with_wrong_parent_source_type(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        parent = common.raw_dataset.copy()
        parent["source"] = {"type": "file"}
        res = dataset_handler.create_dataset(auth_event(parent), None)
        parent_id = json.loads(res["body"])["Id"]

        child = common.raw_dataset.copy()
        child["parent_id"] = parent_id

        res = dataset_handler.create_dataset(auth_event(child), None)
        assert res["statusCode"] == 400
        assert (
            "wrong parent source type" in json.loads(res["body"])[0]["message"].lower()
        )

    def test_create_geo(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        create_event = auth_event(common.raw_geo_dataset.copy())
        response = dataset_handler.create_dataset(create_event, None)
        body = json.loads(response["body"])
        dataset_id = body["Id"]

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]

        assert item["spatial"] == ["Bydel Østafor", "Bydel Vestafor"]
        assert item["spatialResolutionInMeters"] == Decimal("720.31")
        assert item["conformsTo"] == ["EUREF89 UTM sone 32, 2d"]

    def test_create_geo_with_source_reference(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset_source = {"type": "database", "database": "geodata", "table": "benches"}
        dataset = common.raw_geo_dataset.copy()
        dataset["source"] = dataset_source
        create_event = auth_event(dataset)
        response = dataset_handler.create_dataset(create_event, None)
        body = json.loads(response["body"])
        dataset_id = body["Id"]

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]

        assert item["source"] == dataset_source

    def test_create_geo_invalid(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        invalid_dataset = common.raw_geo_dataset.copy()
        invalid_dataset["spatialResolutionInMeters"] = -12.2
        create_event = auth_event(invalid_dataset)
        response = dataset_handler.create_dataset(create_event, None)
        error_message = json.loads(response["body"])
        assert response["statusCode"] == 400
        assert error_message == {
            "message": "Validation error",
            "errors": [
                "spatialResolutionInMeters: -12.2 is less than the minimum of 0"
            ],
        }


class TestUpdateDataset:
    def test_update_dataset(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]
        event_for_update = auth_event(common.dataset_updated.copy(), dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        body = json.loads(response["body"])

        assert body["Id"] == dataset_id
        assert response["statusCode"] == 200

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item[ID_COLUMN] == dataset_id
        assert item[TYPE_COLUMN] == "Dataset"
        assert item["title"] == "UPDATED TITLE"
        assert item["accrualPeriodicity"] == "daily"

    def test_forbidden(self, event, metadata_table, auth_event):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]
        event_for_update = event(common.dataset_updated, dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {"message": f"You are not authorized to access dataset {dataset_id}"}
        ]

    def test_update_invalid(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        dataset_handler.create_dataset(auth_event(dataset), None)

        invalid_dataset = dataset.copy()
        invalid_dataset["accessRights"] = "foo"
        update_event = auth_event(invalid_dataset)

        response = dataset_handler.update_dataset(update_event, None)
        error_message = json.loads(response["body"])
        assert response["statusCode"] == 400
        assert error_message == {
            "message": "Validation error",
            "errors": [
                "accessRights: 'foo' is not one of ['non-public', 'public', 'restricted']"
            ],
        }

    def test_update_invalid_change(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]

        invalid_dataset = dataset.copy()
        invalid_dataset["parent_id"] = "dataset-42"
        update_event = auth_event(invalid_dataset, dataset_id)

        response = dataset_handler.update_dataset(update_event, None)
        error_message = json.loads(response["body"])
        assert response["statusCode"] == 400
        assert error_message == {
            "message": "The value of parent_id cannot be changed.",
        }

    def test_dataset_not_exist(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset_id = "dataset-id"
        event_for_update = auth_event(common.dataset_updated, dataset_id)

        response = dataset_handler.update_dataset(event_for_update, None)
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]

    def test_slugify(self):
        title = (
            "  Tittel på datasett 42 med spesialtegn :+*/\\_[](){} og norske tegn ÆØÅ  "
        )
        result = dataset_repository.slugify(title)

        assert result == "tittel-pa-datasett-42-med-spesialtegn-og-norske-tegn-eoa"

    def test_update_geo_dataset(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_geo_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]
        event_for_update = auth_event(common.updated_geo_dataset.copy(), dataset_id)
        response = dataset_handler.update_dataset(event_for_update, None)

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]

        assert item[ID_COLUMN] == dataset_id
        assert item["contactPoint"]["name"] == "Timian"
        assert item["spatial"] == ["Oslo"]
        assert item["spatialResolutionInMeters"] == Decimal("500")
        assert item["license"] == "https://data.norge.no/nlod/no/2.0"
        assert "conformsTo" not in item

    def test_update_geo_dataset_with_invalid_source_reference(
        self, auth_event, metadata_table
    ):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_geo_dataset.copy()
        dataset["source"] = {
            "type": "database",
            "database": "geodata",
            "table": "benches",
        }
        response = dataset_handler.create_dataset(auth_event(dataset), None)
        body = json.loads(response["body"])
        dataset_id = body["Id"]

        updated_dataset = common.updated_geo_dataset.copy()
        updated_dataset["source"] = {"foo": "bar"}
        event_for_update = auth_event(updated_dataset, dataset_id)
        response = dataset_handler.update_dataset(event_for_update, None)

        assert response["statusCode"] == 400
        assert json.loads(response["body"]) == {
            "message": "Validation error",
            "errors": ["source: 'type' is a required property"],
        }


class TestPatchDataset:
    def test_patch_dataset(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]
        event_for_patch = auth_event(common.dataset_patched.copy(), dataset_id)

        response = dataset_handler.patch_dataset(event_for_patch, None)
        body = json.loads(response["body"])

        assert body["Id"] == dataset_id
        assert response["statusCode"] == 200

        db_response = metadata_table.query(
            KeyConditionExpression=Key(ID_COLUMN).eq(dataset_id)
        )
        item = db_response["Items"][0]
        assert item[ID_COLUMN] == dataset_id
        assert item[TYPE_COLUMN] == "Dataset"
        # Changed:
        assert item["title"] == "PATCHED TITLE"
        assert item["keywords"][0] == "saksbehandling"
        assert item["contactPoint"]["name"] == "Kim"
        # Unchanged:
        assert item["accrualPeriodicity"] == "hourly"
        assert item["objective"] == "Formålsbeskrivelse"

    def test_forbidden(self, event, metadata_table, auth_event):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)

        body = json.loads(response["body"])
        dataset_id = body["Id"]
        event_for_patch = event(common.dataset_patched.copy(), dataset_id)

        response = dataset_handler.patch_dataset(event_for_patch, None)
        assert response["statusCode"] == 403
        assert json.loads(response["body"]) == [
            {"message": f"You are not authorized to access dataset {dataset_id}"}
        ]

    def test_update_invalid(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        dataset_handler.create_dataset(auth_event(dataset), None)

        invalid_dataset = common.dataset_patched.copy()
        invalid_dataset["foo"] = "bar"
        update_event = auth_event(invalid_dataset)

        response = dataset_handler.patch_dataset(update_event, None)
        error_message = json.loads(response["body"])
        assert response["statusCode"] == 400
        assert error_message == {
            "message": "Validation error",
            "errors": [
                "Additional properties are not allowed ('foo' was unexpected)",
            ],
        }

    def test_dataset_not_exist(self, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset_id = "dataset-id"
        event_for_patch = auth_event(common.dataset_patched, dataset_id)

        response = dataset_handler.patch_dataset(event_for_patch, None)
        assert response["statusCode"] == 404
        assert json.loads(response["body"]) == [
            {"message": f"Dataset {dataset_id} does not exist"}
        ]


class TestGetDataset:
    def test_get_all_datasets(self, event, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        dataset["source"] = {"type": "none"}
        response = dataset_handler.create_dataset(auth_event(dataset), None)
        dataset_id = json.loads(response["body"])["Id"]

        for i in range(0, 3):
            child_dataset = common.raw_dataset.copy()
            child_dataset["parent_id"] = dataset_id

            response = dataset_handler.create_dataset(auth_event(child_dataset), None)

        response = dataset_handler.get_datasets(event(), None)
        datasets = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(datasets) == 4  # Including parent dataset

    def test_get_datasets_by_parent(self, event, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        dataset["source"] = {"type": "none"}
        response = dataset_handler.create_dataset(auth_event(dataset), None)
        dataset_id = json.loads(response["body"])["Id"]

        for i in range(0, 3):
            child_dataset = common.raw_dataset.copy()
            child_dataset["parent_id"] = dataset_id

            response = dataset_handler.create_dataset(auth_event(child_dataset), None)

        event_for_get = event(query_params={"parent_id": dataset_id})
        response = dataset_handler.get_datasets(event_for_get, None)
        datasets = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(datasets) == 3

    def test_get_datasets_by_parent_none_found(self, event, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)
        dataset_id = json.loads(response["body"])["Id"]

        for i in range(0, 3):
            child_dataset = common.raw_dataset.copy()
            child_dataset["parent_id"] = dataset_id

            response = dataset_handler.create_dataset(auth_event(child_dataset), None)

        event_for_get = event(query_params={"parent_id": "no-children"})
        response = dataset_handler.get_datasets(event_for_get, None)
        datasets = json.loads(response["body"])

        assert response["statusCode"] == 200
        assert len(datasets) == 0

    def test_get_dataset(self, event, auth_event, metadata_table):
        import metadata.dataset.handler as dataset_handler

        dataset = common.raw_geo_dataset.copy()
        response = dataset_handler.create_dataset(auth_event(dataset), None)
        body = json.loads(response["body"])
        dataset_id = body["Id"]

        event_for_get = event(dataset=dataset_id)
        response = dataset_handler.get_dataset(event_for_get, None)

        assert response["statusCode"] == 200
        dataset = json.loads(response["body"])
        assert dataset["Id"] == "akebakker-under-kommunal-forvaltning-i-oslo"
        assert dataset["spatial"] == ["Bydel Østafor", "Bydel Vestafor"]
        assert dataset["spatialResolutionInMeters"] == 720.31
        assert dataset["conformsTo"] == ["EUREF89 UTM sone 32, 2d"]
