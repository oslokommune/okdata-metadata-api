import boto3
from boto3.dynamodb.conditions import Key
import re
import shortuuid

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


def create_dataset(content):
    title = content["title"]
    dataset_id = generate_unique_id_based_on_title(title)

    content[common.DATASET_ID] = dataset_id
    dataset_table.put_item(Item=content)
    return dataset_id


def update_dataset(dataset_id, content):
    content[common.DATASET_ID] = dataset_id
    db_response = dataset_table.put_item(Item=content)
    return db_response


def generate_unique_id_based_on_title(title):
    id = slugify(title)[:50]
    if dataset_exists(id):
        return id + "-" + shortuuid.ShortUUID().random(length=5)
    else:
        return id


def slugify(title):
    a = 'àáäâãåăçèéëêæǵḧìíïîḿńǹñòóöôœøṕŕßśșțùúüûǘẃẍÿź_'
    b = 'aaaaaaaceeeeeghiiiimnnnooooooprssstuuuuuwxyz '
    tr = str.maketrans(a, b)
    t = re.sub('\W+', '-', title.lower().translate(tr))
    if t[0] == '-':
        t = t[1:]
    if t[-1] == '-':
        t = t[0:-1]
    return t