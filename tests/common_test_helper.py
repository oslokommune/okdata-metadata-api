from copy import deepcopy

import common as table

dataset_table_name = "metadata-api-dataset"
version_table_name = "metadata-api-version"
edition_table_name = "metadata-api-edition"
distribution_table_name = "metadata-api-distribution"
metadata_table_name = "dataset-metadata"


def create_metadata_table(dynamodb):
    return create_table(
        dynamodb, metadata_table_name, table.ID_COLUMN, table.TYPE_COLUMN
    )


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

dataset_new_format = remove_ids(dataset)
dataset_new_format[table.ID_COLUMN] = dataset[table.DATASET_ID]
dataset_new_format[table.TYPE_COLUMN] = "Dataset"

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

version = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "version": "6",
    "schema": {},
    "transformation": {},
}

version_new_format = remove_ids(version)
version_new_format[
    table.ID_COLUMN
] = f"{dataset[table.DATASET_ID]}#{version['version']}"
version_new_format[table.TYPE_COLUMN] = "Version"

new_version = remove_ids(version)

version_updated = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-UPDATED",
    "version": "6-TEST",
    "schema": {},
    "transformation": {},
}

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
