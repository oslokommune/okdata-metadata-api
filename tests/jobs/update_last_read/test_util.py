from jobs.update_last_read.util import _timestamp_path, s3_path


def test_timestamp_path():
    assert _timestamp_path("2020-03-06") == "year=2020/month=3/day=6"
    assert _timestamp_path("2020-03-06-12") == "year=2020/month=3/day=6/hour=12"


def test_s3_path():
    assert (
        s3_path("test", "raw", "red", "dataplatform-s3-logs", "2020-01-01", "data.csv")
        == "test-data-bucket/test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=1/day=1/data.csv"
    )
    assert (
        s3_path(
            "test", "raw", "red", "dataplatform-s3-logs", "2020-01-01-12", "data.csv"
        )
        == "test-data-bucket/test/raw/red/dataplatform/dataplatform-s3-logs/version=1/year=2020/month=1/day=1/hour=12/data.csv"
    )
