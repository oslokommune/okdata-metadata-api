# -*- coding: utf-8 -*-

import boto3
import shortuuid
import simplejson as json
from boto3.dynamodb.conditions import Key

import common
import common as table

dynamodb = boto3.resource('dynamodb', 'eu-west-1')

table_name_prefix = "metadata-api"
version_table = dynamodb.Table(table_name_prefix + "-version")
dataset_table = dynamodb.Table(table_name_prefix + "-dataset")


def post_version(event, context):
    """POST /datasets/:dataset-id/versions"""

    body_as_json = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    body_as_json[table.DATASET_ID] = dataset_id

    version_from_body = body_as_json["version"]
    unique_id = generate_unique_id(version_from_body)
    body_as_json[table.VERSION_ID] = unique_id
    if not dataset_exists(dataset_id):
        return common.response(404, "Selected dataset does not exist. Could not post version.")

    db_response = version_table.put_item(Item=body_as_json)

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"], unique_id)


def update_version(event, context):
    """PUT /datasets/:dataset-id/versions/:version-id"""

    body_as_json = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]

    if not dataset_exists(dataset_id):
        return common.response(404, "Selected dataset does not exist. Could not update version.")

    if not version_exists(version_id):
        return common.response(404, "Selected version does not exist. Could not update version.")

    body_as_json[table.DATASET_ID] = dataset_id
    body_as_json[table.VERSION_ID] = version_id
    db_response = version_table.put_item(Item=body_as_json)

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"], version_id)


def get_versions(event, context):
    """GET /datasets/:dataset-id/versions"""

    dataset_id = event["pathParameters"]["dataset-id"]

    db_response = version_table.scan(
        FilterExpression=Key(table.DATASET_ID).eq(dataset_id)
    )

    body = db_response["Items"]
    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"], body)


def get_version(event, context):
    """GET /datasets/:dataset-id/versions/:version-id"""

    dataset_id = event["pathParameters"]["dataset-id"]
    version_id = event["pathParameters"]["version-id"]

    db_response = version_table.scan(
        FilterExpression=Key(table.VERSION_ID).eq(version_id) & Key(table.DATASET_ID).eq(dataset_id)
    )

    if len(db_response["Items"]) == 0:
        return common.response(404, "Selected version does not exist.")

    body = db_response["Items"][0]

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"], body)


def version_exists(version_name):
    version_table = dynamodb.Table(table_name_prefix + "-version")

    db_response = version_table.query(
        KeyConditionExpression=Key(table.VERSION_ID).eq(version_name)
    )

    return len(db_response["Items"]) > 0


def dataset_exists(dataset_name):
    db_response = dataset_table.query(
        KeyConditionExpression=Key(table.DATASET_ID).eq(dataset_name)
    )

    return len(db_response["Items"]) > 0


def generate_unique_id(type_string):
    return type_string + "-" + shortuuid.ShortUUID().random(length=8)
