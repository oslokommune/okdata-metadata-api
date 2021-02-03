from functools import wraps

import simplejson as json
from botocore.config import Config

from metadata.auth import is_dataset_owner
from metadata.dataset.repository import DatasetRepository

ID_COLUMN = "Id"
TYPE_COLUMN = "Type"

BOTO_RESOURCE_COMMON_KWARGS = {
    "region_name": "eu-west-1",
    "config": Config(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
        retries={
            "mode": "standard",
        }
    ),
}

table_name_prefix = "metadata-api"


def validate_input(validator):
    def inner(func):
        @wraps(func)
        def wrapper(event, *args, **kwargs):
            errors = validator.validate(json.loads(event["body"]))
            if errors:
                return response(400, {"message": "Validation error", "errors": errors})
            return func(event, *args, **kwargs)

        return wrapper

    return inner


def check_auth(func):
    def wrapper(event, *args, **kwargs):
        path_parameters = event["pathParameters"]
        dataset_id = path_parameters["dataset-id"]

        if DatasetRepository().get_dataset(dataset_id) is None:
            message = f"Dataset {dataset_id} does not exist"
            return error_response(404, message)

        auth_header = event["headers"].get("Authorization")
        if not auth_header:
            message = "Authorization header missing"
            return error_response(403, message)

        bearer_token = auth_header.split(" ")[-1]
        if not is_dataset_owner(bearer_token, dataset_id):
            message = f"You are not authorized to access dataset {dataset_id}"
            return error_response(403, message)

        return func(event, *args, **kwargs)

    return wrapper


def response(statusCode, body, headers=None):
    if not headers:
        headers = {}

    headers["Access-Control-Allow-Origin"] = "*"

    return {
        "statusCode": statusCode,
        "headers": headers,
        "body": json.dumps(body, use_decimal=True),
    }


def error_response(statusCode, body, headers=None):
    if isinstance(body, list):
        return response(statusCode, body, headers)
    if isinstance(body, dict):
        return response(statusCode, [body], headers)
    tmp = []
    tmp.append({"message": body})
    return response(statusCode, tmp, headers)
