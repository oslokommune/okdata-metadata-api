import os
import re
import json
import pytest
import boto3
from moto import mock_dynamodb2
from auth import SimpleAuth

from tests import common_test_helper
from metadata.dataset import handler as dataset_handler
from metadata.version import handler as version_handler
from metadata.edition import handler as edition_handler

AUTHORIZER_API = os.environ["AUTHORIZER_API"]
good_token = "Bjørnepollett"


@pytest.fixture()
def dynamodb():
    with mock_dynamodb2():
        yield boto3.resource("dynamodb", "eu-west-1")


@pytest.fixture(autouse=True)
def auth_mock(requests_mock, mocker):
    matcher = re.compile(f"{AUTHORIZER_API}/.*")

    requests_mock.register_uri("GET", matcher, json={"access": False})
    requests_mock.register_uri(
        "GET",
        matcher,
        request_headers={"Authorization": good_token},
        json={"access": True},
    )
    mocker.patch.object(SimpleAuth, "request_from_client")
    mocker.patch.object(SimpleAuth, "request_with_token_exchange")
    mocker.patch.object(SimpleAuth, "is_owner", return_value=True)


@pytest.fixture()
def auth_denied(mocker):
    mocker.patch.object(SimpleAuth, "request_from_client")
    mocker.patch.object(SimpleAuth, "request_with_token_exchange")
    mocker.patch.object(SimpleAuth, "is_owner", return_value=False)


def lambda_event_factory(token=None):
    def _lambda_event(
        body={}, dataset=None, version=None, edition=None, distribution=None
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

        event = {"body": json.dumps(body)}

        if token:
            event["headers"] = {"Authorization": token}

        if path:
            event["pathParameters"] = path

        event["requestContext"] = {"authorizer": {"principalId": "mock-user"}}
        return event

    return _lambda_event


@pytest.fixture()
def event():
    return lambda_event_factory()


@pytest.fixture()
def auth_event(*args, **kwargs):
    return lambda_event_factory(good_token)


@pytest.fixture()
def put_dataset(auth_event):
    dataset = common_test_helper.raw_dataset.copy()
    response = dataset_handler.create_dataset(auth_event(dataset), None)
    body = json.loads(response["body"])
    return body["Id"]


@pytest.fixture()
def put_version(auth_event, put_dataset):
    dataset_id = put_dataset
    raw_version = common_test_helper.raw_version.copy()
    version_handler.create_version(auth_event(raw_version, dataset=dataset_id), None)
    return dataset_id, raw_version["version"]


@pytest.fixture()
def put_edition(auth_event, put_version):
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
