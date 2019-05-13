import boto3
from boto3.dynamodb.conditions import Key

import common
import dataset_repository


dynamodb = boto3.resource("dynamodb", "eu-west-1")

version_table = dynamodb.Table(common.table_name_prefix + "-version")
metadata_table = dynamodb.Table("dataset-metadata")


def version_exists(dataset_id, version_id):
    version = get_version(dataset_id, version_id)
    return version is not None


def get_version(dataset_id, version_id):
    try:
        id = f"{dataset_id}#{version_id}"
        db_response = metadata_table.query(
            KeyConditionExpression=Key(common.ID_COLUMN).eq(id)
        )
        items = db_response["Items"]
    except Exception:
        items = None

    if not items:
        # Fall back to legacy version table
        try:
            db_response = version_table.query(
                KeyConditionExpression=Key(common.VERSION_ID).eq(version_id)
            )
            items = db_response["Items"]
        except Exception:
            pass

    if len(items) == 0:
        return None
    elif len(items) > 1:
        raise Exception(f"Illegal state: Multiple versions with id {version_id}")
    else:
        return items[0]


def get_versions(dataset_id):
    try:
        type_cond = Key(common.TYPE_COLUMN).eq("Version")
        id_cond = Key(common.ID_COLUMN).begins_with(f"{dataset_id}#")

        db_response = metadata_table.query(
            IndexName="IdByTypeIndex", KeyConditionExpression=type_cond & id_cond
        )
        items = db_response["Items"]
    except Exception:
        items = []

    if not items:
        # Fall back to legacy version table
        db_response = version_table.scan(
            FilterExpression=Key(common.DATASET_ID).eq(dataset_id)
        )
        items = db_response["Items"]

    return items


def create_version(dataset_id, content):
    if not dataset_repository.dataset_exists(dataset_id):
        return None

    version = content["version"]
    if version_exists(dataset_id, version):
        return None  # TODO return error message

    version_id = f"{dataset_id}#{version}"
    content[common.ID_COLUMN] = version_id
    content[common.TYPE_COLUMN] = "Version"

    db_response = metadata_table.put_item(Item=content)
    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    if http_status == 200:
        return version_id
    else:
        return None


def update_version(dataset_id, version_id, content):
    old_version = get_version(dataset_id, version_id)
    if not old_version:
        return False

    content[common.DATASET_ID] = old_version[common.DATASET_ID]
    content[common.VERSION_ID] = version_id

    db_response = version_table.put_item(Item=content)

    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    return http_status == 200
