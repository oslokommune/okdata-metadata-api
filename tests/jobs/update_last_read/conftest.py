import pytest

from metadata.util import getenv
from tests.common_test_helper import create_metadata_table


@pytest.fixture
def s3_logs_bucket(s3_client):
    bucket_name = getenv("LOGS_BUCKET_NAME")
    s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": getenv("AWS_REGION")},
    )
    return bucket_name


@pytest.fixture
def metadata_table(dynamodb):
    return create_metadata_table(dynamodb)
