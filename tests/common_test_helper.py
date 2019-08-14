from copy import deepcopy

import metadata.common as table

dataset_table_name = "metadata-api-dataset"
version_table_name = "metadata-api-version"
edition_table_name = "metadata-api-edition"
distribution_table_name = "metadata-api-distribution"
metadata_table_name = "dataset-metadata"


def create_metadata_table(dynamodb):
    return create_table(
        dynamodb,
        metadata_table_name,
        table.ID_COLUMN,
        table.TYPE_COLUMN,
        gsi="IdByTypeIndex",
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


def create_table(dynamodb, table_name, hashkey, rangekey=None, gsi=None):
    keyschema = [{"AttributeName": hashkey, "KeyType": "HASH"}]
    attributes = [{"AttributeName": hashkey, "AttributeType": "S"}]
    gsis = []

    if rangekey:
        keyschema.append({"AttributeName": rangekey, "KeyType": "RANGE"})
        attributes.append({"AttributeName": rangekey, "AttributeType": "S"})

    if gsi:
        gsis = [
            {
                "IndexName": gsi,
                "KeySchema": [
                    {"AttributeName": rangekey, "KeyType": "HASH"},
                    {"AttributeName": hashkey, "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "INCLUDE"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            }
        ]

    return dynamodb.create_table(
        TableName=table_name,
        KeySchema=keyschema,
        AttributeDefinitions=attributes,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        GlobalSecondaryIndexes=gsis,
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
version_new_format[table.ID_COLUMN] = f"{dataset[table.DATASET_ID]}/6"
version_new_format[table.TYPE_COLUMN] = "Version"

new_version = remove_ids(version)

version_updated = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-UPDATED",
    "version": "6-TEST",
    "schema": "new schema",
    "transformation": {},
}

next_version_new_format = remove_ids(version_updated)
next_version_new_format[table.ID_COLUMN] = f"{dataset[table.DATASET_ID]}/7"
next_version_new_format[table.TYPE_COLUMN] = "Version"
next_version_new_format["version"] = "7"

edition = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "editionID": "EDITION-id",
    "edition": "2019-05-28T15:37:00+02:00",
    "description": "Data for one hour",
    "startTime": "2018-12-21T08:00:00+01:00",
    "endTime": "2018-12-21T09:00:00+01:00",
}

new_edition = remove_ids(edition)

edition_id_new = f"{dataset[table.DATASET_ID]}/{version['version']}/20190528T133700"
edition_new_format = remove_ids(edition)
edition_new_format[table.ID_COLUMN] = edition_id_new
edition_new_format[table.TYPE_COLUMN] = "Edition"

edition_updated = {
    "datasetID": "antall-besokende-pa-gjenbruksstasjoner",
    "versionID": "6-TEST",
    "editionID": "EDITION-id-updated",
    "edition": "2019-05-28T15:37:00+02:00",
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

distribution_id_new = f"{dataset[table.DATASET_ID]}/{version['version']}/20190528T133700/e80b5f2c-67f0-4a50-a6d9-b6a565ef2401"
distribution_new_format = remove_ids(distribution)
distribution_new_format[table.ID_COLUMN] = distribution_id_new
distribution_new_format[table.TYPE_COLUMN] = "Distribution"

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
