from jobs.update_last_read.logrec import LogRecord


def test_clean_field():
    # `_clean_field` should just return its argument in most cases ...
    assert LogRecord._clean_field("") == ""
    assert LogRecord._clean_field("foo") == "foo"
    # ... but remove stuff that S3 uses to denote "blank".
    assert LogRecord._clean_field("-") == ""


def test_datetime():
    with open("tests/jobs/update_last_read/data/s3_access_log.txt") as f:
        lines = f.read().split("\n")

    log_record = LogRecord.from_log_line(lines[0])

    assert log_record.datetime().isoformat() == "2020-02-17T06:31:44+00:00"


def test_from_log_line():
    with open("tests/jobs/update_last_read/data/s3_access_log.txt") as f:
        lines = f.read()

    log_records = [LogRecord.from_log_line(line) for line in lines.strip().split("\n")]

    assert len(log_records) == 4 and all(
        [isinstance(lr, LogRecord) for lr in log_records]
    )
