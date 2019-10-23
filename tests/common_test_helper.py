import metadata.common as table

metadata_table_name = "dataset-metadata"


def create_metadata_table(dynamodb):
    return create_table(
        dynamodb,
        metadata_table_name,
        table.ID_COLUMN,
        table.TYPE_COLUMN,
        gsi="IdByTypeIndex",
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


raw_dataset = {
    "title": "Antall besøkende på gjenbruksstasjoner",
    "description": "Sensordata fra tellere på gjenbruksstasjonene",
    "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
    "accrualPeriodicity": "hourly",
    "accessRights": "non-public",
    "confidentiality": "green",
    "processing_stage": "raw",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "98765432",
    },
    "publisher": "REN",
}

dataset_updated = {
    "title": "UPDATED TITLE",
    "description": "Sensordata fra tellere på gamle gjenbruksstasjoner",
    "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
    "accrualPeriodicity": "hourly",
    "accessRights": "restricted",
    "confidentiality": "red",
    "processing_stage": "raw",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "12345678",
    },
    "publisher": "REN",
}

raw_version = {"version": "6"}

version_updated = {"version": "6-TEST"}

raw_edition = {
    "edition": "2019-05-28T15:37:00+02:00",
    "description": "Data for one hour",
    "startTime": "2018-12-21",
    "endTime": "2018-12-21",
}

edition_updated = {
    "edition": "2019-05-28T15:37:00+02:00",
    "description": "CHANGED",
    "startTime": "2018-12-21",
    "endTime": "2018-12-21",
}

raw_distribution = {"filename": "BOOOM.csv"}


distribution_updated = {"filename": "UPDATED.csv"}
