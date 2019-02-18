import random
import string

import boto3
import shortuuid
import simplejson as json
from boto3.dynamodb.conditions import Key

import common
import common as table

dynamodb = boto3.resource('dynamodb', 'eu-west-1')

table_name_prefix = "metadata-api"
distribution_table = dynamodb.Table(table_name_prefix + "-distribution")


def post_distribution(event, context):
    """POST /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions"""

    body_as_json = json.loads(event["body"])

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]
    edition_id = event["pathParameters"]["edition-id"]

    body_as_json[table.DATASET_ID] = dataset_id
    body_as_json[table.VERSION_ID] = version_id
    body_as_json[table.EDITION_ID] = edition_id

    unique_id = generate_unique_id("DISTR")
    body_as_json[table.DISTRIBUTION_ID] = unique_id

    if not dataset_exists(dataset_id):
        return common.response(404, "Selected dataset does not exist.")

    if not version_exists(version_id):
        return common.response(404, "Selected version does not exist.")

    if not edition_exists(edition_id):
        return common.response(404, "Selected edition does not exist.")

    db_response = distribution_table.put_item(Item=body_as_json)

    body = unique_id

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           body)


def update_distribution(event, context):
    """PUT /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions/:distribution-id"""

    body_as_json = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]
    edition_id = event["pathParameters"]["edition-id"]
    distribution_id = event["pathParameters"]["distribution-id"]

    if not dataset_exists(dataset_id):
        return common.response(404, "Selected dataset does not exist.")

    if not version_exists(version_id):
        return common.response(404, "Selected version does not exist.")

    if not edition_exists(edition_id):
        return common.response(404, "Selected edition does not exist.")

    if not distribution_exists(distribution_id):
        return common.response(404, "Selected distribution does not exist.")

    body_as_json[table.DATASET_ID] = dataset_id
    body_as_json[table.VERSION_ID] = version_id
    body_as_json[table.EDITION_ID] = edition_id
    body_as_json[table.DISTRIBUTION_ID] = distribution_id

    db_response = distribution_table.put_item(Item=body_as_json)

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           distribution_id)


def get_distributions(event, context):
    """GET /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]
    edition_id = event["pathParameters"]["edition-id"]

    fe = Key(table.EDITION_ID).eq(edition_id) & Key(table.DATASET_ID).eq(dataset_id) & Key(table.VERSION_ID).eq(version_id)

    db_response = distribution_table.scan(FilterExpression=fe)

    body = db_response["Items"]

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           body)


def get_distribution(event, context):
    """GET /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions/:distribution-id"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]
    edition_id = event["pathParameters"]["edition-id"]
    distribution_id = event["pathParameters"]["distribution-id"]

    fe = Key(table.DISTRIBUTION_ID).eq(distribution_id) & Key(table.EDITION_ID).eq(edition_id) & Key(table.DATASET_ID).eq(
        dataset_id) & Key(table.VERSION_ID).eq(version_id)
    db_response = distribution_table.scan(FilterExpression=fe)

    if len(db_response["Items"]) == 0:
        return common.response(404, "Selected distribution does not exist.")

    body = db_response["Items"]

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           body)


def distribution_exists(distribution_name):
    edition_table = dynamodb.Table(table_name_prefix + "-distribution")

    db_response = edition_table.query(
        KeyConditionExpression=Key(table.DISTRIBUTION_ID).eq(distribution_name)
    )

    return len(db_response["Items"]) > 0


def edition_exists(edition_name):
    edition_table = dynamodb.Table(table_name_prefix + "-edition")

    db_response = edition_table.query(
        KeyConditionExpression=Key(table.EDITION_ID).eq(edition_name)
    )

    return len(db_response["Items"]) > 0


def version_exists(version_name):
    version_table = dynamodb.Table(table_name_prefix + "-version")

    db_response = version_table.query(
        KeyConditionExpression=Key(table.VERSION_ID).eq(version_name)
    )

    return len(db_response["Items"]) > 0


def dataset_exists(dataset_name):
    dataset_table = dynamodb.Table(table_name_prefix + "-dataset")

    db_response = dataset_table.query(
        KeyConditionExpression=Key(table.DATASET_ID).eq(dataset_name)
    )

    return len(db_response["Items"]) > 0


def random_char(number_of_characters):
    return ''.join(random.choice(string.ascii_letters) for x in range(number_of_characters))


def generate_unique_id(type_string):
    return type_string + "-" + shortuuid.ShortUUID().random(length=5)
