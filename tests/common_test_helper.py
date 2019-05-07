from copy import deepcopy
import json

import common as table
import dataset_handler
import distribution_handler
import edition_handler
import version_handler

dataset_table_name = "metadata-api-dataset"
version_table_name = "metadata-api-version"
edition_table_name = "metadata-api-edition"
distribution_table_name = "metadata-api-distribution"


def read_result_body(response):
    return json.loads(response["body"])


def post_dataset(*dataset_events):
    return [dataset_handler.post_dataset(event, None) for event in dataset_events]


def post_version(d_event, *version_events):
    response_from_dataset_post = post_dataset(d_event)
    response_from_dataset_post = response_from_dataset_post[0]
    assert response_from_dataset_post["statusCode"] == 200

    dataset_id = read_result_body(response_from_dataset_post)

    version_responses = []
    for v_event in version_events:
        v_event["pathParameters"] = {"dataset-id": dataset_id}
        version_responses.append(version_handler.post_version(v_event, None))

    return response_from_dataset_post, version_responses


def post_edition(d_event, v_event, *edition_events):
    response_from_dataset_post, response_from_version_post = post_version(
        d_event, v_event
    )
    response_from_version_post = response_from_version_post[0]
    assert response_from_version_post["statusCode"] == 200

    dataset_id = read_result_body(response_from_dataset_post)
    version_id = read_result_body(response_from_version_post)

    edition_responses = []
    for e_event in edition_events:
        e_event["pathParameters"] = {"version-id": version_id, "dataset-id": dataset_id}
        edition_responses.append(edition_handler.post_edition(e_event, None))
    return response_from_dataset_post, response_from_version_post, edition_responses


def post_distribution(d_event, v_event, e_event, *distribution_events):
    d_res, v_res, e_res = post_edition(d_event, v_event, e_event)
    e_res = e_res[0]
    assert e_res["statusCode"] == 200

    dataset_id = read_result_body(d_res)
    version_id = read_result_body(v_res)
    edition_id = read_result_body(e_res)

    distribution_responses = []
    for distribution_event in distribution_events:
        distribution_event["pathParameters"] = {
            "version-id": version_id,
            "dataset-id": dataset_id,
            "edition-id": edition_id,
        }
        distribution_responses.append(
            distribution_handler.post_distribution(distribution_event, None)
        )

    return d_res, v_res, e_res, distribution_responses


def create_dataset_table(dynamodb):
    return create_table(dynamodb, dataset_table_name, table.DATASET_ID)


def create_version_table(dynamodb):
    return create_table(
        dynamodb, version_table_name, table.VERSION_ID, table.DATASET_ID
    )


def create_edition_table(dynamodb):
    return create_table(
        dynamodb, edition_table_name, table.EDITION_ID, table.DATASET_ID
    )


def create_distribution_table(dynamodb):
    return create_table(
        dynamodb, distribution_table_name, table.DISTRIBUTION_ID, table.DATASET_ID
    )


def create_table(dynamodb, table_name, hashkey, rangekey=None):
    keyschema = [{"AttributeName": hashkey, "KeyType": "HASH"}]
    attributes = [{"AttributeName": hashkey, "AttributeType": "S"}]

    if rangekey:
        keyschema.append({"AttributeName": rangekey, "KeyType": "RANGE"})
        attributes.append({"AttributeName": rangekey, "AttributeType": "S"})

    return dynamodb.create_table(
        TableName=table_name,
        KeySchema=keyschema,
        AttributeDefinitions=attributes,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )


def remove_ids(item):
    result = deepcopy(item)
    result.pop(table.DATASET_ID, None)
    result.pop(table.VERSION_ID, None)
    result.pop(table.EDITION_ID, None)
    result.pop(table.DISTRIBUTION_ID, None)
    return result


dataset = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "title": "Antall besøkende på gjenbruksstasjoner",
    "description": "Sensordata fra tellere på gjenbruksstasjonene",
    "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
    "frequency": "hourly",
    "accessRights": ":non-public",
    "privacyLevel": "green",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "98765432",
    },
    "publisher": "REN",
}

new_dataset = remove_ids(dataset)

dataset_updated = {
    "datasetID": "updated-title",
    "title": "UPDATED TITLE",
    "description": "Sensordata fra tellere på gamle gjenbruksstasjoner",
    "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
    "frequency": "hourly",
    "accessRights": ":restricted",
    "privacyLevel": "red",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "12345678",
    },
    "publisher": "REN",
}

dataset_event = {"body": json.dumps(dataset)}

version = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "version": "6",
    "schema": {},
    "transformation": {},
}

new_version = remove_ids(version)

version_updated = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-UPDATED",
    "version": "6-TEST",
    "schema": {},
    "transformation": {},
}

version_event = {"body": json.dumps(version)}

edition = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "editionID": "EDITION-id",
    "description": "Data for one hour",
    "startTime": "2018-12-21T08:00:00+01:00",
    "endTime": "2018-12-21T09:00:00+01:00",
}

new_edition = remove_ids(edition)

edition_updated = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "editionID": "EDITION-id-updated",
    "description": "CHANGED",
    "startTime": "2018-12-21T08:00:00+01:00",
    "endTime": "2018-12-21T09:00:00+01:00",
}

edition_event = {"body": json.dumps(edition)}

distribution = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "editionID": "EDITION-id",
    "distributionID": "DISTRIBUTION-xyz",
    "filename": "BOOOM.csv",
    "format": "text/csv",
    "checksum": "...",
}

new_distribution = remove_ids(distribution)

distribution_updated = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "editionID": "EDITION-id",
    "distributionID": "DISTRIBUTION-abc",
    "filename": "UPDATED.csv",
    "format": "text/csv",
    "checksum": "...",
}

distribution_event = {"body": json.dumps(distribution)}
