import boto3
import shortuuid
from boto3.dynamodb.conditions import Key

import common
import dataset_repository
import version_repository
import edition_repository

dynamodb = boto3.resource("dynamodb", "eu-west-1")

distribution_table = dynamodb.Table(common.table_name_prefix + "-distribution")
metadata_table = dynamodb.Table("dataset-metadata")


def distribution_exists(dataset_id, version_id, edition_id, distribution_id):
    distribution = get_distribution(dataset_id, version_id, edition_id, distribution_id)
    return distribution is not None


def get_distribution(dataset_id, version_id, edition_id, distribution_id):
    try:
        id = f"{dataset_id}#{version_id}#{edition_id}#{distribution_id}"
        db_response = metadata_table.query(
            KeyConditionExpression=Key(common.ID_COLUMN).eq(id)
        )
        items = db_response["Items"]
    except Exception:
        items = []

    if not items:
        # Fall back to legacy distribution table
        db_response = distribution_table.query(
            KeyConditionExpression=Key(common.DISTRIBUTION_ID).eq(distribution_id)
        )
        items = db_response["Items"]

    if len(items) == 0:
        return None
    elif len(items) > 1:
        raise Exception(
            f"Illegal state: Multiple distributions with id {distribution_id}"
        )
    else:
        return items[0]


def get_distributions(dataset_id, version_id, edition_id):
    try:
        type_cond = Key(common.TYPE_COLUMN).eq("Distribution")
        id_prefix = f"{dataset_id}#{version_id}#{edition_id}#"
        id_cond = Key(common.ID_COLUMN).begins_with(id_prefix)

        db_response = metadata_table.query(
            IndexName="IdByTypeIndex", KeyConditionExpression=type_cond & id_cond
        )
        items = db_response["Items"]
    except Exception:
        items = []

    if not items:
        # Fall back to legacy edition table
        fe = (
            Key(common.EDITION_ID).eq(edition_id)
            & Key(common.DATASET_ID).eq(dataset_id)
            & Key(common.VERSION_ID).eq(version_id)
        )

        db_response = distribution_table.scan(FilterExpression=fe)
        items = db_response["Items"]

    return items


def create_distribution(dataset_id, version_id, edition_id, content):
    content[common.DATASET_ID] = dataset_id
    content[common.VERSION_ID] = version_id
    content[common.EDITION_ID] = edition_id

    distribution_id = generate_unique_id("DISTR")
    content[common.DISTRIBUTION_ID] = distribution_id

    if not dataset_repository.dataset_exists(dataset_id):
        return None

    if not version_repository.version_exists(dataset_id, version_id):
        return None

    if not edition_repository.edition_exists(dataset_id, version_id, edition_id):
        return None

    db_response = distribution_table.put_item(Item=content)

    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    if http_status == 200:
        return distribution_id
    else:
        return None


def update_distribution(dataset_id, version_id, edition_id, distribution_id, content):
    old_distribution = get_distribution(
        dataset_id, version_id, edition_id, distribution_id
    )
    if not old_distribution:
        return False

    content[common.DATASET_ID] = old_distribution[common.DATASET_ID]
    content[common.VERSION_ID] = old_distribution[common.VERSION_ID]
    content[common.EDITION_ID] = old_distribution[common.EDITION_ID]
    content[common.DISTRIBUTION_ID] = distribution_id

    db_response = distribution_table.put_item(Item=content)

    http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

    return http_status == 200


def generate_unique_id(type_string):
    return type_string + "-" + shortuuid.ShortUUID().random(length=5)
