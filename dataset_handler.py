from difflib import SequenceMatcher

import boto3
import shortuuid
import simplejson as json
from boto3.dynamodb.conditions import Key

import common

dynamodb = boto3.resource('dynamodb', 'eu-west-1')
table_name_prefix = "metadata-api"
dataset_table = dynamodb.Table(table_name_prefix + "-dataset")


def post_dataset(event, context):
    """POST /datasets"""

    body_as_json = json.loads(event["body"])
    title_from_body = body_as_json["title"]
    unique_id = generate_unique_id_based_on_title(title_from_body)
    body_as_json[common.DATASET_ID] = unique_id
    db_response = dataset_table.put_item(Item=body_as_json)
    body = unique_id

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           body)


def update_dataset(event, context):
    """PUT /datasets:dataset-id"""

    body_as_json = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    if not dataset_exists(dataset_id):
        return common.response(404, "Selected dataset does not exist. Could not update dataset.")

    body_as_json[common.DATASET_ID] = dataset_id

    db_response = dataset_table.put_item(Item=body_as_json)

    body = dataset_id

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           body)


def get_datasets(event, context):
    """GET /datasets"""

    db_response = dataset_table.scan()

    body = db_response["Items"]

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           body)


def get_dataset(event, context):
    """GET /datasets/:dataset-id"""

    dataset_id = event["pathParameters"]["dataset-id"]

    if not dataset_exists(dataset_id):
        return common.response(404, "Selected dataset does not exist.")

    db_response = dataset_table.query(
        KeyConditionExpression=Key(common.DATASET_ID).eq(dataset_id)
    )

    body = db_response["Items"]

    return common.response(db_response["ResponseMetadata"]["HTTPStatusCode"],
                           body)


def dataset_exists(dataset_name):
    db_response = dataset_table.query(
        KeyConditionExpression=Key(common.DATASET_ID).eq(dataset_name)
    )

    return len(db_response["Items"]) > 0


def generate_unique_id_based_on_title(title):
    return (title.replace(" ", "-").replace("ø", "oe").replace("å", "aa").replace("æ", "ae"))[
           :30] + "-" + shortuuid.ShortUUID().random(length=5)


def check_similarity_to_other_datasets(input_json):
    already_existing_datasets = dataset_table.scan()["Items"]
    print("--- Datasets ---")
    for dataset in already_existing_datasets:
        print("-----" + dataset["title"])
        for item in input_json:
            print(item + ": " + str(similar(str(dataset[item]), str(input_json[item]))))


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()
