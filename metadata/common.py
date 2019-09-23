import simplejson as json
from jsonschema import ValidationError

ID_COLUMN = "Id"
TYPE_COLUMN = "Type"
DATASET_ID = "datasetID"
VERSION_ID = "versionID"
EDITION_ID = "editionID"
DISTRIBUTION_ID = "distributionID"

table_name_prefix = "metadata-api"


def validate_input(validator):
    def inner(func):
        def wrapper(event, *args, **kwargs):
            try:
                validator.validate(json.loads(event["body"]))
            except ValidationError as ve:
                return response(400, ve.message)
            return func(event, *args, **kwargs)

        return wrapper

    return inner


def response(statusCode, body, headers=None):
    if not headers:
        headers = {}

    headers["Access-Control-Allow-Origin"] = "*"

    return {"statusCode": statusCode, "headers": headers, "body": json.dumps(body)}
