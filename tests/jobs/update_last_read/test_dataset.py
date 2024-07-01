from jobs.update_last_read.dataset import DatasetEntry


def test_from_s3_key_valid():
    key = "raw/green/befolkningsframskrivninger/version=1/edition=20200207T070503/Befolkningsframskrivning(Fram 2019).csv"
    entry = DatasetEntry.from_s3_key(key)
    assert entry.stage == "raw"
    assert entry.confidentiality == "green"
    assert entry.dataset_id == "befolkningsframskrivninger"
    assert entry.version == "1"
    assert entry.edition_path == "edition=20200207T070503"
    assert entry.filename == "Befolkningsframskrivning(Fram 2019).csv"


def test_from_s3_key_filename_with_whitespace():
    key = "raw/green/befolkningsframskrivninger/version=1/edition=20200207T070503/Befolkningsframskrivning(Fram 2019).csv"
    entry = DatasetEntry.from_s3_key(key)
    assert entry.stage == "raw"
    assert entry.confidentiality == "green"
    assert entry.dataset_id == "befolkningsframskrivninger"
    assert entry.version == "1"
    assert entry.edition_path == "edition=20200207T070503"
    assert entry.filename == "Befolkningsframskrivning(Fram 2019).csv"


def test_from_s3_key_invalid_stage():
    key = "foo/green/levekar-trangbodde-historisk/version=1/edition=20190531T090056/14.json"
    assert not DatasetEntry.from_s3_key(key)


def test_from_s3_key_non_parsing():
    key = "processed/green/levekar-trangbodde-historisk/15.json"
    assert not DatasetEntry.from_s3_key(key)
