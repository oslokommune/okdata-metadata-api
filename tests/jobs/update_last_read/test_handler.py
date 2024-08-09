from boto3.dynamodb.conditions import Key
from freezegun import freeze_time

from jobs.update_last_read.handler import handler, _two_hours_ago
from metadata.CommonRepository import ID_COLUMN


@freeze_time("2020-01-02-12")
def test_two_hours_ago():
    assert _two_hours_ago() == "2020-01-02-10"


@freeze_time("2020-01-01-02")
def test_handler(s3_client, s3_bucket, metadata_table):
    metadata_table.put_item(Item={"Id": "renovasjonsbiler-status", "Type": "Dataset"})
    metadata_table.put_item(Item={"Id": "pipeline-ng-test", "Type": "Dataset"})

    with open("tests/jobs/update_last_read/data/s3_access_log.txt", "rb") as f:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key="logs/s3/test-data-bucket/2020-01-01-00-00-00-E3257304837D19F5",
            Body=f,
        )

    handler({}, {})

    res = metadata_table.query(
        KeyConditionExpression=Key(ID_COLUMN).eq("renovasjonsbiler-status")
    )
    assert res["Items"][0]["last_read"] == "2020-02-17T06:31:44+00:00"

    res = metadata_table.query(
        KeyConditionExpression=Key(ID_COLUMN).eq("pipeline-ng-test")
    )
    assert "last_read" not in res["Items"][0]
