import json
from functools import wraps

import simplejson
from botocore.config import Config


BOTO_RESOURCE_COMMON_KWARGS = {
    "region_name": "eu-west-1",
    "config": Config(
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/retries.html
        retries={
            "mode": "standard",
            "max_attempts": 5,
        }
    ),
}

# The three standard processing stages in the dataplatform. Corresponds to what
# is called "bronze", "silver", and "gold" in the medallion architecture.
STAGES = ["raw", "intermediate", "processed"]

# The three standard confidentiality levels in the dataplatform. "Green" is
# open/public data, "yellow" is Origo internal, while "red" has restricted
# access.
CONFIDENTIALITY_MAP = {
    "public": "green",
    "restricted": "yellow",
    "non-public": "red",
}

CONFIDENTIALITIES = list(CONFIDENTIALITY_MAP.values())


def validate_input(validator):
    def inner(func):
        @wraps(func)
        def wrapper(event, *args, **kwargs):
            try:
                errors = validator.validate(json.loads(event["body"]))
            except json.decoder.JSONDecodeError as e:
                return response(
                    400, {"message": "JSON parse error", "errors": [str(e)]}
                )

            if errors:
                # TODO: A 422 response is probably more accurate here (correct
                # syntax, but invalid content).
                return response(400, {"message": "Validation error", "errors": errors})
            return func(event, *args, **kwargs)

        return wrapper

    return inner


def response(statusCode, body, headers=None):
    if not headers:
        headers = {}

    headers["Access-Control-Allow-Origin"] = "*"

    return {
        "statusCode": statusCode,
        "headers": headers,
        "body": simplejson.dumps(body, use_decimal=True),
    }


def error_response(statusCode, body, headers=None):
    if isinstance(body, list):
        return response(statusCode, body, headers)
    if isinstance(body, dict):
        return response(statusCode, [body], headers)
    tmp = []
    tmp.append({"message": body})
    return response(statusCode, tmp, headers)
