import json
import os
import re
from unittest import mock

import boto3
import jsonschema
import pytest
from moto import mock_aws
from okdata.resource_auth import ResourceAuthorizer

from metadata.auth import Auth
from metadata.common import BOTO_RESOURCE_COMMON_KWARGS
from metadata.util import getenv
from tests import common_test_helper


OKDATA_PERMISSION_API_URL = os.environ["OKDATA_PERMISSION_API_URL"]
good_token = "Bjørnepollett"


@pytest.fixture()
def dynamodb():
    with mock_aws():
        yield boto3.resource("dynamodb", **BOTO_RESOURCE_COMMON_KWARGS)


@pytest.fixture(scope="function")
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def s3_bucket(s3_client):
    bucket_name = getenv("DATA_BUCKET_NAME")
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": getenv("AWS_REGION")},
    )
    return bucket_name


@pytest.fixture(autouse=True)
def auth_mock(requests_mock, mocker, monkeypatch):
    client_credentials_header = {
        "Authorization": "Bearer Magic-keycloak-stuff",
    }

    def has_access_mock(
        self, bearer_token, scope, resource_name=None, use_whitelist=False
    ):
        return (
            bearer_token == good_token
            and resource_name.startswith("okdata:dataset:")
            and (scope == "okdata:dataset:update" or scope == "okdata:dataset:write")
        )

    monkeypatch.setattr(ResourceAuthorizer, "has_access", has_access_mock)
    mocker.patch.object(
        Auth,
        "service_client_authorization_header",
        return_value=client_credentials_header,
    )


@pytest.fixture(autouse=True)
def mock_client_secret():
    with mock.patch("metadata.auth.get_secret") as get_secret:
        get_secret.return_value = "abc123"
        yield get_secret


@pytest.fixture(autouse=True)
def okdata_permission_api_mock(requests_mock, mocker):
    url_matcher = re.compile(f"{OKDATA_PERMISSION_API_URL}/permissions")

    def body_matcher(request):
        body = request.json()
        schema = {
            "title": "CreateResourceBody",
            "type": "object",
            "properties": {
                "owner": {"$ref": "#/definitions/User"},
                "resource_name": {"title": "Resource Name", "type": "string"},
            },
            "required": ["owner", "resource_name"],
            "definitions": {
                "User": {
                    "title": "User",
                    "type": "object",
                    "properties": {
                        "user_id": {"title": "User Id", "type": "string"},
                        "user_type": {"title": "User Type", "type": "string"},
                    },
                    "required": ["user_id", "user_type"],
                }
            },
        }
        jsonschema.validate(body, schema)
        return True

    client_credentials_header = {
        "Authorization": "Bearer Magic-keycloak-stuff",
    }

    requests_mock.register_uri(
        "POST",
        url_matcher,
        request_headers=client_credentials_header,
        status_code=201,
        additional_matcher=body_matcher,
    )


def lambda_event_factory(token=None):
    def _lambda_event(
        body={},
        dataset=None,
        version=None,
        edition=None,
        distribution=None,
        query_params=None,
    ):
        path = {}
        if dataset:
            path["dataset-id"] = dataset
        if version:
            path["version"] = version
        if edition:
            path["edition"] = edition
        if distribution:
            path["distribution"] = distribution

        event = {
            "body": json.dumps(body),
            "headers": {"Authorization": "blarf"},
        }

        if token:
            event["headers"]["Authorization"] = f"Bearer {token}"

        if path:
            event["pathParameters"] = path

        event["queryStringParameters"] = query_params

        event["requestContext"] = {"authorizer": {"principalId": "mock-user"}}
        return event

    return _lambda_event


@pytest.fixture()
def event():
    return lambda_event_factory()


@pytest.fixture()
def auth_event(*args, **kwargs):
    return lambda_event_factory(good_token)


@pytest.fixture
def raw_dataset():
    return {
        "title": "Antall besøkende på gjenbruksstasjoner",
        "description": "Sensordata fra tellere på gjenbruksstasjonene",
        "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
        "accrualPeriodicity": "hourly",
        "accessRights": "non-public",
        "objective": "Formålsbeskrivelse",
        "license": "http://data.norge.no/nlod/no/1.0/",
        "contactPoint": {
            "name": "Tim",
            "email": "tim@oslo.kommune.no",
            "phone": "98765432",
        },
        "publisher": "REN",
    }


@pytest.fixture()
def put_dataset(auth_event, raw_dataset):
    from metadata.dataset import handler as dataset_handler

    response = dataset_handler.create_dataset(auth_event(raw_dataset), None)
    body = json.loads(response["body"])
    return body["Id"]


@pytest.fixture()
def put_version(auth_event, put_dataset):
    from metadata.version import handler as version_handler

    dataset_id = put_dataset
    raw_version = common_test_helper.raw_version.copy()
    version_handler.create_version(auth_event(raw_version, dataset=dataset_id), None)
    return dataset_id, raw_version["version"]


@pytest.fixture()
def put_edition(auth_event, put_version):
    from metadata.edition import handler as edition_handler

    dataset_id, version = put_version
    raw_edition = common_test_helper.raw_edition.copy()
    response = edition_handler.create_edition(
        auth_event(raw_edition, dataset=dataset_id, version=version), None
    )
    body = json.loads(response["body"])

    # When we have a auth_denied call we must check if we get a Id key before accessing it
    edition_id = ""
    if "Id" in body:
        edition_id = body["Id"]
    return dataset_id, version, edition_id.split("/")[-1]
