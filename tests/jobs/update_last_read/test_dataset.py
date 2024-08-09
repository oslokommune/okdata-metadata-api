import pytest

from jobs.update_last_read.dataset import DatasetEntry


@pytest.mark.parametrize(
    "filepath",
    [
        "data.txt",  # Plain
        "Befolkningsframskrivning(Fram 2019).csv",  # With whitespace
        "_delta_log/00000000000000000000.json",  # Multipart
    ],
)
def test_from_s3_key_valid(filepath):
    key = f"raw/green/befolkningsframskrivninger/version=1/edition=20200207T070503/{filepath}"
    entry = DatasetEntry.from_s3_key(key)
    assert entry.stage == "raw"
    assert entry.confidentiality == "green"
    assert entry.dataset_id == "befolkningsframskrivninger"
    assert entry.version == "1"
    assert entry.edition == "20200207T070503"
    assert entry.filepath == filepath


def test_from_s3_key_invalid_stage():
    key = "foo/green/levekar-trangbodde-historisk/version=1/20190531T090056/14.json"
    assert not DatasetEntry.from_s3_key(key)


def test_from_s3_key_non_parsing():
    key = "processed/green/levekar-trangbodde-historisk/15.json"
    assert not DatasetEntry.from_s3_key(key)
