import pytest

from metadata.validator import Validator

dataset = Validator("dataset")
version = Validator("version")
edition = Validator("edition")
distribution = Validator("distribution")

valid_dataset = {
    "theme": ["environment"],
    "contactPoint": {
        "name": "Origo Dataplattform",
        "email": "dataplattform@oslo.kommune.no",
    },
    "publisher": "Origo",
    "keywords": ["badetemperatur"],
    "objective": "Tilgjengeliggjøre badetemperaturer",
    "description": "Badetemperatur sensordata fra Vicotee",
    "title": "Badetemperatur sensordata fra Vicotee",
    "accessRights": "public",
}

wrong_theme = valid_dataset.copy()
wrong_theme["theme"] = ["invalid choice"]

extra_properties = valid_dataset.copy()
extra_properties["unknownProperty"] = "extra extra"

multiple_errors = valid_dataset.copy()
multiple_errors.pop("title")
multiple_errors["accessRights"] = "wrong"


def test_valid_dataset():
    errors = dataset.validate(valid_dataset)
    assert len(errors) == 0


def test_invalid_dataset():
    errors = dataset.validate(wrong_theme)
    assert len(errors) == 1
    assert errors[0].startswith("theme.0: 'invalid choice' is not one of")


def test_extra_properties_dataset():
    errors = dataset.validate(extra_properties)
    assert len(errors) == 1
    assert errors[0].startswith("Additional properties are not allowed")


def test_missing_schema():
    with pytest.raises(Exception, match="Missing schema for object d4t4s3t"):
        Validator("d4t4s3t")


def test_multiple_errors():
    errors = dataset.validate(multiple_errors)
    assert len(errors) == 2
    assert errors[0].startswith("accessRights: 'wrong' is not one of")
    assert errors[1] == "'title' is a required property"
