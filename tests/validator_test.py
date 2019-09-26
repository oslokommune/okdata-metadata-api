import pytest
from jsonschema import ValidationError

from metadata.validator import Validator

dataset = Validator("dataset")
version = Validator("version")
edition = Validator("edition")
distribution = Validator("distribution")

valid_dataset = {
    "theme": "environment",
    "processing_stage": "raw",
    "contactPoint": {
        "name": "Origo Dataplattform",
        "email": "dataplattform@oslo.kommune.no",
    },
    "publisher": "Origo",
    "confidentiality": "green",
    "keywords": ["badetemperatur"],
    "objective": "Tilgjengeliggjøre badetemperaturer",
    "description": "Badetemperatur sensordata fra Vicotee",
    "title": "Badetemperatur sensordata fra Vicotee",
    "accessRights": "public",
}

wrong_theme = valid_dataset.copy()
wrong_theme["theme"] = "invalid choice"

extra_properties = valid_dataset.copy()
extra_properties["unknownProperty"] = "extra extra"


def test_valid_dataset():
    try:
        dataset.validate(valid_dataset)
    except ValidationError as ve:
        print(ve)
        assert 0


def test_invalid_dataset():
    with pytest.raises(ValidationError):
        dataset.validate(wrong_theme)


def test_extra_properties_dataset():
    with pytest.raises(ValidationError):
        dataset.validate(extra_properties)


def test_missing_schema():
    with pytest.raises(Exception, match="Missing schema for object d4t4s3t"):
        Validator("d4t4s3t")
