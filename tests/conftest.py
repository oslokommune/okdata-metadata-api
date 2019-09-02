import os
import re
import json
import pytest
import boto3
from moto import mock_dynamodb2
from layer.auth import SimpleAuth

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
    mocker.patch.object(SimpleAuth, "requests")
    mocker.patch.object(SimpleAuth, "is_owner", return_value=True)


@pytest.fixture()
def auth_denied(mocker):
    mocker.patch.object(SimpleAuth, "requests")
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
def auth_event():
    return lambda_event_factory(good_token)
