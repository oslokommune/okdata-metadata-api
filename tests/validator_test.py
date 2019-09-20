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

invalid_dataset = valid_dataset.copy()
invalid_dataset["theme"] = "invalid choice"


def test_valid_dataset():
    try:
        dataset.validate(valid_dataset)
    except ValidationError as ve:
        print(ve)
        assert 0


def test_invalid_dataset():
    with pytest.raises(ValidationError):
        dataset.validate(invalid_dataset)
