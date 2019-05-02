from difflib import SequenceMatcher

import re
import shortuuid
import simplejson as json

import common
import dataset_repository


def post_dataset(event, context):
    """POST /datasets"""

    body_as_json = json.loads(event["body"])
    title_from_body = body_as_json["title"]
    unique_id = generate_unique_id_based_on_title(title_from_body)

    db_response = dataset_repository.create_dataset(unique_id, body_as_json)

    return common.response(200, unique_id)


def update_dataset(event, context):
    """PUT /datasets/:dataset-id"""

    body_as_json = json.loads(event["body"])
    dataset_id = event["pathParameters"]["dataset-id"]

    if not dataset_repository.dataset_exists(dataset_id):
        return common.response(404, "Selected dataset does not exist. Could not update dataset.")

    dataset_repository.update_dataset(dataset_id, body_as_json)

    return common.response(200, dataset_id)


def get_datasets(event, context):
    """GET /datasets"""

    body = dataset_repository.get_datasets()

    return common.response(200, body)


def get_dataset(event, context):
    """GET /datasets/:dataset-id"""

    dataset_id = event["pathParameters"]["dataset-id"]
    dataset = dataset_repository.get_dataset(dataset_id)

    if dataset:
        return common.response(200, dataset)
    else:
        return common.response(404, "Selected dataset does not exist.")


def generate_unique_id_based_on_title(title):
    id = slugify(title)[:50]
    if dataset_repository.dataset_exists(id):
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


def check_similarity_to_other_datasets(input_json):
    already_existing_datasets = dataset_table.scan()["Items"]
    print("--- Datasets ---")
    for dataset in already_existing_datasets:
        print("-----" + dataset["title"])
        for item in input_json:
            print(item + ": " + str(similar(str(dataset[item]), str(input_json[item]))))


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()
