import simplejson as json
from metadata.dataset.repository import DatasetRepository
from metadata.version.repository import VersionRepository
from metadata.edition.repository import EditionRepository

ID_COLUMN = "Id"
TYPE_COLUMN = "Type"

table_name_prefix = "metadata-api"


def validate_input(validator):
    def inner(func):
        def wrapper(event, *args, **kwargs):
            errors = validator.validate(json.loads(event["body"]))
            if errors:
                return response(400, {"message": "Validation error", "errors": errors})
            return func(event, *args, **kwargs)

        return wrapper

    return inner


def resources_exists(
    dataset_required=False, version_required=False, edition_required=False
):
    def inner(func):
        def wrapper(event, *args, **kwargs):
            path_parameters = event["pathParameters"]
            if dataset_required:
                dataset_id = path_parameters["dataset-id"]
                if DatasetRepository().get_dataset(dataset_id) is None:
                    message = f"Dataset {dataset_id} does not exist"
                    return response(404, {"message": message})
            if version_required:
                dataset_id = path_parameters["dataset-id"]
                version = path_parameters["version"]
                if VersionRepository().get_version() is None:
                    message = f"Version {dataset_id}/{version} does not exist"
                    return response(404, {"message": message})
            if edition_required:
                dataset_id = path_parameters["dataset-id"]
                version = path_parameters["version"]
                edition = path_parameters["edition"]
                if EditionRepository().get_edition(dataset_id, version, edition) is None:
                    message = f"Edition {dataset_id}/{version}/{edition} does not exist"
                    return response(404, {"message": message})
            return func(event, *args, **kwargs)

        return wrapper

    return inner


def response(statusCode, body, headers=None):
    if not headers:
        headers = {}

    headers["Access-Control-Allow-Origin"] = "*"

    return {"statusCode": statusCode, "headers": headers, "body": json.dumps(body)}


def error_response(statusCode, body, headers=None):
    if isinstance(body, list):
        return response(statusCode, body, headers)
    if isinstance(body, dict):
        return response(statusCode, [body], headers)
    tmp = []
    tmp.append({"message": body})
    return response(statusCode, tmp, headers)
