from jobs.update_last_read.logrec import LogRecord


def test_clean_field():
    # `_clean_field` should just return its argument in most cases ...
    assert LogRecord._clean_field("") == ""
    assert LogRecord._clean_field("foo") == "foo"
    # ... but remove stuff that S3 uses to denote "blank".
    assert LogRecord._clean_field("-") == ""


def test_from_log_line():
    with open("tests/jobs/update_last_read/data/s3_access_log.txt") as f:
        lines = f.read()
        log_records = [
            LogRecord.from_log_line(line) for line in lines.strip().split("\n")
        ]
        assert log_records
