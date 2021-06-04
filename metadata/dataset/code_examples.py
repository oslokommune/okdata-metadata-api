"""Generation of code examples tailored for specific datasets.

The main entry point of this module is the `code_examples` function, which
takes a dataset ID as a parameter and returns a suitable Python code example
for it, if possible, otherwise it raises `NoCodeExamples`.
"""

from metadata.dataset.repository import DatasetRepository
from metadata.distribution.repository import DistributionRepository
from metadata.edition.repository import EditionRepository
from metadata.version.repository import VersionRepository


def __mock_generate__(config):
    """Return example code based on `config`.

    TODO: Remove when the real generation code is on place.
    """
    return {
        "content_type": config["content_type"],
        "code": "print('hello, world')",
    }


class NoCodeExamples(Exception):
    """Raised when sensible code examples can't be produced for a dataset."""

    pass


def code_examples(dataset_id):
    """Return a list of code examples for `dataset_id`."""

    dataset = DatasetRepository().get_dataset(dataset_id)
    if not dataset:
        raise NoCodeExamples("Dataset not found")

    version = VersionRepository().get_version(dataset_id, "latest")
    if not version:
        raise NoCodeExamples("No version found for dataset")

    version_id = version["version"]

    edition = EditionRepository().get_edition(dataset_id, version_id, "latest")
    if not edition:
        raise NoCodeExamples(f"No edition found for version {version_id}")

    edition_id = edition["Id"].split("/")[2]

    distributions = DistributionRepository().get_distributions(
        dataset_id, version_id, edition_id
    )
    if not distributions:
        raise NoCodeExamples(
            f"No distributions found for edition {version_id}/{edition_id}"
        )

    return [
        __mock_generate__(
            {
                "dataset_id": dataset_id,
                "version": version_id,
                "dataset_type": distribution.get("distribution_type"),
                "content_type": distribution.get("content_type"),
                "access_rights": dataset["accessRights"],
            }
        )
        for distribution in distributions
    ]
