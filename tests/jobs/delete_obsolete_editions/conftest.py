import pytest


@pytest.fixture()
def editions():
    return [
        {
            "edition": "2023-01-01T08:00:00+00:00",
            "Id": "foo/1/20230101T080000",
            "Type": "Edition",
        },
        {
            "edition": "2023-01-05T08:00:00+00:00",
            "Id": "foo/1/20230105T080000",
            "Type": "Edition",
        },
        {
            "edition": "2023-01-10T08:00:00+00:00",
            "Id": "foo/1/20230110T080000",
            "Type": "Edition",
        },
        {
            "edition": "2023-01-15T08:00:00+00:00",
            "Id": "foo/1/20230115T080000",
            "Type": "Edition",
        },
        {
            "edition": "2023-01-20T08:00:00+00:00",
            "Id": "foo/1/20230120T080000",
            "Type": "Edition",
        },
    ]
