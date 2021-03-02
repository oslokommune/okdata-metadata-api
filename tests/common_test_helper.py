from metadata.CommonRepository import ID_COLUMN, TYPE_COLUMN

metadata_table_name = "dataset-metadata"


def create_metadata_table(dynamodb):
    return create_table(
        dynamodb,
        metadata_table_name,
        ID_COLUMN,
        TYPE_COLUMN,
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
    "objective": "Formålsbeskrivelse",
    "license": "http://data.norge.no/nlod/no/1.0/",
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
    "accrualPeriodicity": "daily",
    "accessRights": "non-public",
    "objective": "Formålsbeskrivelse",
    "license": "http://data.norge.no/nlod/no/2.0/",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "12345678",
    },
    "publisher": "REN",
}

dataset_patched = {
    "title": "PATCHED TITLE",
    "keywords": ["saksbehandling", "forankring", "ressursinnsats"],
    "contactPoint": {
        "name": "Kim",
        "email": "kim@oslo.kommune.no",
        "phone": "12345678",
    },
}

raw_version = {"version": "6"}

version_updated = {"version": "6"}

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

raw_file_distribution = {
    "distribution_type": "file",
    "content_type": "text/csv",
    "filename": "BOOOM.csv",
    "filenames": ["BOOOM.csv"],
}

raw_api_distribution = {
    "distribution_type": "api",
    "content_type": "text/csv",
    "api_url": "https://example.org",
}

distribution_updated = {
    "distribution_type": "file",
    "content_type": "text/csv",
    "filename": "UPDATED.csv",
    "filenames": ["BOOOM.csv"],
}

raw_geo_dataset = {
    "title": "Akebakker under kommunal forvaltning i Oslo",
    "description": "Oversikt over akebakker som er under kommunal forvalting i Oslo",
    "keywords": ["adspredelse", "bymiljø", "geodata"],
    "accrualPeriodicity": "irregular",
    "accessRights": "public",
    "objective": "Formidle akebakker",
    "spatial": ["Bydel Østafor", "Bydel Vestafor"],
    "spatialResolutionInMeters": 720.31,
    "conformsTo": ["EUREF89 UTM sone 32, 2d"],
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "98765432",
    },
    "publisher": "BYM",
}

updated_geo_dataset = {
    "title": "Akebakker under kommunal forvaltning i Oslo",
    "description": "Oversikt over akebakker som er under kommunal forvalting i Oslo",
    "keywords": ["adspredelse", "bymiljø", "geodata"],
    "accrualPeriodicity": "irregular",
    "accessRights": "public",
    "objective": "Formidle akebakker",
    "spatial": ["Oslo"],
    "spatialResolutionInMeters": 500,
    "license": "http://data.norge.no/nlod/no/2.0/",
    "contactPoint": {
        "name": "Timian",
        "email": "tim@oslo.kommune.no",
        "phone": "98765432",
    },
    "publisher": "BYM",
}


class Context:
    def __init__(self, aws_request_id):
        self.aws_request_id = aws_request_id
