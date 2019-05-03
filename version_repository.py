import boto3
from boto3.dynamodb.conditions import Key
import shortuuid

import common


dynamodb = boto3.resource("dynamodb", "eu-west-1")

version_table = dynamodb.Table(common.table_name_prefix + "-version")


def version_exists(version_id):
    version = get_version(version_id)
    return version is not None


def get_version(version_id):
    db_response = version_table.query(
        KeyConditionExpression=Key(common.VERSION_ID).eq(version_id)
    )
    items = db_response["Items"]

    if len(items) == 0:
        return None
    elif len(items) > 1:
        raise Exception(f"Illegal state: Multiple versions with id {version_id}")
    else:
        return items[0]


def get_versions(dataset_id):
    db_response = version_table.scan(
        FilterExpression=Key(common.DATASET_ID).eq(dataset_id)
    )
    items = db_response["Items"]
    return items


def create_version(dataset_id, content):
    version = content["version"]
    version_id = generate_unique_id(version)

    content[common.DATASET_ID] = dataset_id
    content[common.VERSION_ID] = version_id
    db_response = version_table.put_item(Item=content)

    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    if http_status == 200:
        return version_id
    else:
        return None


def update_version(version_id, content):
    old_version = get_version(version_id)
    if not old_version:
        return False

    content[common.DATASET_ID] = old_version[common.DATASET_ID]
    content[common.VERSION_ID] = version_id

    db_response = version_table.put_item(Item=content)

    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    return http_status == 200


def generate_unique_id(version):
    return version + "-" + shortuuid.ShortUUID().random(length=8)
