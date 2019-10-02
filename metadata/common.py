import simplejson as json

ID_COLUMN = "Id"
TYPE_COLUMN = "Type"

table_name_prefix = "metadata-api"


def validate_input(validator):
    def inner(func):
        def wrapper(event, *args, **kwargs):
            errors = validator.validate(json.loads(event["body"]))
            if errors:
                return response(400, {"errors": errors})
            return func(event, *args, **kwargs)

        return wrapper

    return inner


def response(statusCode, body, headers=None):
    if not headers:
        headers = {}

    headers["Access-Control-Allow-Origin"] = "*"

    return {"statusCode": statusCode, "headers": headers, "body": json.dumps(body)}
