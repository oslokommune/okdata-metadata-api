import simplejson as json

DATASET_ID = "datasetID"
VERSION_ID = "versionID"
EDITION_ID = "editionID"
DISTRIBUTION_ID = "distributionID"

table_name_prefix = "metadata-api"


def response(statusCode, body):
    return {
        "statusCode": statusCode,
        "headers": {
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }
