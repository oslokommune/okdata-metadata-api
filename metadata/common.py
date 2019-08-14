import simplejson as json

ID_COLUMN = "Id"
TYPE_COLUMN = "Type"
DATASET_ID = "datasetID"
VERSION_ID = "versionID"
EDITION_ID = "editionID"
DISTRIBUTION_ID = "distributionID"

table_name_prefix = "metadata-api"


def response(statusCode, body, headers=None):
    if not headers:
        headers = {}

    headers["Access-Control-Allow-Origin"] = "*"

    return {"statusCode": statusCode, "headers": headers, "body": json.dumps(body)}
