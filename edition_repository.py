import boto3
from boto3.dynamodb.conditions import Key
import shortuuid

import common
import dataset_repository
import version_repository


dynamodb = boto3.resource("dynamodb", "eu-west-1")

edition_table = dynamodb.Table(common.table_name_prefix + "-edition")


def edition_exists(edition_id):
    edition = get_edition(edition_id)
    return edition is not None


def get_edition(edition_id):
    db_response = edition_table.query(
        KeyConditionExpression=Key(common.EDITION_ID).eq(edition_id)
    )
    items = db_response["Items"]

    if len(items) == 0:
        return None
    elif len(items) > 1:
        raise Exception(f"Illegal state: Multiple editions with id {edition_id}")
    else:
        return items[0]


def get_editions(dataset_id, version_id):
    fe = Key(common.DATASET_ID).eq(dataset_id) & Key(common.VERSION_ID).eq(version_id)

    db_response = edition_table.scan(FilterExpression=fe)

    return db_response["Items"]


def create_edition(dataset_id, version_id, content):
    if not dataset_repository.dataset_exists(dataset_id):
        return None

    if not version_repository.version_exists(version_id):
        return None

    content[common.DATASET_ID] = dataset_id
    content[common.VERSION_ID] = version_id

    edition_id = generate_unique_id("EDITION")
    content[common.EDITION_ID] = edition_id

    db_response = edition_table.put_item(Item=content)

    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    if http_status == 200:
        return edition_id
    else:
        return None


def update_edition(edition_id, content):
    old_edition = get_edition(edition_id)
    if not old_edition:
        return False

    content[common.DATASET_ID] = old_edition[common.DATASET_ID]
    content[common.VERSION_ID] = old_edition[common.VERSION_ID]
    content[common.EDITION_ID] = edition_id

    db_response = edition_table.put_item(Item=content)

    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    return http_status == 200


def generate_unique_id(type_string):
    return type_string + "-" + shortuuid.ShortUUID().random(length=5)
