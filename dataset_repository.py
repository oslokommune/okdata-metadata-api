import boto3
from boto3.dynamodb.conditions import Key

import common


dynamodb = boto3.resource('dynamodb', 'eu-west-1')

dataset_table = dynamodb.Table(common.table_name_prefix + "-dataset")


def dataset_exists(dataset_id):
    dataset = get_dataset(dataset_id)
    return dataset is not None


def get_dataset(dataset_id):
	db_response = dataset_table.query(
        KeyConditionExpression=Key(common.DATASET_ID).eq(dataset_id)
    )
	items = db_response["Items"]

	if len(items) == 0:
		return None
	elif len(items) > 1:
		raise Exception(f"Illegal state: Multiple datasets with id {dataset_id}")
	else:
	    return items[0]


def get_datasets():
	db_response = dataset_table.scan()
	items = db_response["Items"]
	return items


def create_dataset(dataset_id, content):
	content[common.DATASET_ID] = dataset_id
	db_response = dataset_table.put_item(Item=content)
	return db_response


def update_dataset(dataset_id, content):
	content[common.DATASET_ID] = dataset_id
	db_response = dataset_table.put_item(Item=content)
	return db_response