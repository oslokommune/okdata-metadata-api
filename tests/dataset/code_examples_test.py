import ast
from unittest.mock import patch

import pytest

from metadata.dataset.code_examples import (
    NoCodeExamples,
    _code_example,
    code_examples,
)

dataset_types = ["file", "api"]
content_types = [
    "application/geo+json",
    "application/json",
    "application/parquet",
    "application/vnd.ms-excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "text/csv",
]
access_rights = ["public", "restricted", "non-public"]


@pytest.mark.parametrize("dataset_type", dataset_types)
@pytest.mark.parametrize("content_type", content_types)
@pytest.mark.parametrize("access_rights", access_rights)
def test_generate_code_example(dataset_type, content_type, access_rights):
    # Code example should parse as valid Python code. This raises `SyntaxError`
    # if not.
    ast.parse(
        _code_example(
            {
                "dataset_id": "my-dataset",
                "version": "1",
                "dataset_type": dataset_type,
                "content_type": content_type,
                "access_rights": access_rights,
            }
        )["code"]
    )


@patch("metadata.dataset.code_examples.DatasetRepository.get_dataset")
def test_code_examples_no_dataset(mock_get_dataset):
    mock_get_dataset.return_value = None

    with pytest.raises(NoCodeExamples):
        code_examples("foo")


@patch("metadata.dataset.code_examples.VersionRepository.get_version")
@patch("metadata.dataset.code_examples.DatasetRepository.get_dataset")
def test_code_examples_no_version(mock_get_dataset, mock_get_version):
    mock_get_dataset.return_value = {"Id": "foo", "accessRights": "public"}
    mock_get_version.return_value = None

    with pytest.raises(NoCodeExamples):
        code_examples("foo")


@patch("metadata.dataset.code_examples.EditionRepository.get_edition")
@patch("metadata.dataset.code_examples.VersionRepository.get_version")
@patch("metadata.dataset.code_examples.DatasetRepository.get_dataset")
def test_code_examples_no_edition(mock_get_dataset, mock_get_version, mock_get_edition):
    mock_get_dataset.return_value = {"Id": "foo", "accessRights": "public"}
    mock_get_version.return_value = {"version": "1"}
    mock_get_edition.return_value = None

    with pytest.raises(NoCodeExamples):
        code_examples("foo")


@patch("metadata.dataset.code_examples.DistributionRepository.get_distributions")
@patch("metadata.dataset.code_examples.EditionRepository.get_edition")
@patch("metadata.dataset.code_examples.VersionRepository.get_version")
@patch("metadata.dataset.code_examples.DatasetRepository.get_dataset")
def test_code_examples_no_distributions(
    mock_get_dataset, mock_get_version, mock_get_edition, mock_get_distributions
):
    mock_get_dataset.return_value = {"Id": "foo", "accessRights": "public"}
    mock_get_version.return_value = {"version": "1"}
    mock_get_edition.return_value = {"Id": "foo/1/20200101"}
    mock_get_distributions.return_value = []

    with pytest.raises(NoCodeExamples):
        code_examples("foo")


@patch("metadata.dataset.code_examples.DistributionRepository.get_distributions")
@patch("metadata.dataset.code_examples.EditionRepository.get_edition")
@patch("metadata.dataset.code_examples.VersionRepository.get_version")
@patch("metadata.dataset.code_examples.DatasetRepository.get_dataset")
def test_code_examples_success(
    mock_get_dataset, mock_get_version, mock_get_edition, mock_get_distributions
):
    mock_get_dataset.return_value = {"Id": "foo", "accessRights": "public"}
    mock_get_version.return_value = {"version": "1"}
    mock_get_edition.return_value = {"Id": "foo/1/20200101"}
    mock_get_distributions.return_value = [
        {
            "distribution_type": "file",
            "content_type": "text/csv",
        },
        {
            "distribution_type": "file",
            "content_type": "application/json",
        },
    ]

    examples = code_examples("foo")
    assert len(examples) == 2
    assert set(e["content_type"] for e in examples) == {"text/csv", "application/json"}
