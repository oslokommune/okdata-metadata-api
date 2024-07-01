import os
from datetime import datetime, timedelta


def yesterday():
    """Return yesterday's date on the format YYYY-MM-DD."""
    return (datetime.now() - timedelta(days=1)).isoformat()[:10]


def getenv(name):
    """Return the environment variable named `name`, or raise OSError if unset."""
    env = os.getenv(name)

    if env is None:
        raise OSError(f"Environment variable {name} is not set")

    return env


def _timestamp_path(timestamp):
    components = list(map(int, timestamp.split("-")))

    if len(components) == 4:
        path = "year={}/month={}/day={}/hour={}"
    else:
        path = "year={}/month={}/day={}"

    return path.format(*components)


def s3_path(prefix, stage, confidentiality, dataset_id, timestamp, filename):
    return os.path.join(
        getenv("DATA_BUCKET_NAME"),
        prefix,
        stage,
        confidentiality,
        "dataplatform",
        dataset_id,
        "version=1",
        _timestamp_path(timestamp),
        filename,
    )
