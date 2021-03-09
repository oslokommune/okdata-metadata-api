from metadata.CommonRepository import ID_COLUMN, TYPE_COLUMN

metadata_table_name = "dataset-metadata"


def create_metadata_table(dynamodb):
    return create_table(
        dynamodb,
        metadata_table_name,
        ID_COLUMN,
        TYPE_COLUMN,
        gsis=[
            {
                "index_name": "IdByTypeIndex",
                "hash_key": TYPE_COLUMN,
                "range_key": ID_COLUMN,
            },
            {
                "index_name": "IdByApiIdSparseIndex",
                "hash_key": "api_id",
            },
        ],
    )


def create_table(dynamodb, table_name, hashkey, rangekey=None, gsis=[]):
    keyschema = [{"AttributeName": hashkey, "KeyType": "HASH"}]
    attributes = [{"AttributeName": hashkey, "AttributeType": "S"}]

    if rangekey:
        keyschema.append({"AttributeName": rangekey, "KeyType": "RANGE"})
        attributes.append({"AttributeName": rangekey, "AttributeType": "S"})

    for gsi in gsis:
        attribute_names = [attr["AttributeName"] for attr in attributes]

        if gsi["hash_key"] not in attribute_names:
            attributes.append({"AttributeName": gsi["hash_key"], "AttributeType": "S"})

        if "range_key" in gsi and gsi["range_key"] not in attribute_names:
            attributes.append({"AttributeName": gsi["range_key"], "AttributeType": "S"})

    return dynamodb.create_table(
        TableName=table_name,
        KeySchema=keyschema,
        AttributeDefinitions=attributes,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        GlobalSecondaryIndexes=[
            {
                "IndexName": gsi["index_name"],
                "KeySchema": [
                    {"AttributeName": gsi["hash_key"], "KeyType": "HASH"},
                    *(
                        [{"AttributeName": gsi["range_key"], "KeyType": "RANGE"}]
                        if "range_key" in gsi
                        else []
                    ),
                ],
                "Projection": {"ProjectionType": "INCLUDE"},
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                },
            }
            for gsi in gsis
        ],
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
    "filename": "BOOOM.csv",
    "filenames": ["BOOOM.csv"],
}

raw_api_distribution = {
    "distribution_type": "api",
    "content_type": "text/csv",
    "api_url": "https://example.org",
    "api_id": "okdata-api-catalog:123",
}

distribution_updated = {
    "distribution_type": "file",
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
