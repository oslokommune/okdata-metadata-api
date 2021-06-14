"""Generation of code examples tailored for specific datasets.

The main entry point of this module is the `code_examples` function, which
takes a dataset ID as a parameter and returns a suitable Python code example
for it, if possible, otherwise it raises `NoCodeExamples`.
"""

from pathlib import Path
from urllib.parse import parse_qs, urlparse

import isort
import jinja2
from black import FileMode, format_str

from metadata.dataset.repository import DatasetRepository
from metadata.distribution.repository import DistributionRepository
from metadata.edition.repository import EditionRepository
from metadata.version.repository import VersionRepository

dataset_types = ["file", "api"]
content_types = {
    "application/geo+json": "geojson",
    "application/json": "json",
    "application/parquet": "parquet",
    "application/vnd.ms-excel": "xls",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
    "text/csv": "csv",
}

template_env = jinja2.Environment(
    loader=jinja2.FileSystemLoader(
        searchpath=f"{Path(__file__).parents[2]}/templates",
    ),
    extensions=["jinja2.ext.do"],
    trim_blocks=True,
)
template = template_env.get_template("main.jinja")


class NoCodeExamples(Exception):
    """Raised when sensible code examples can't be produced for a dataset."""

    pass


def _extract_query(url):
    """Return `url` decomposed into a tuple of base URL and query.

    The base URL is the same URL as passed to the function, but with any query
    string removed, while the query is a dictionary of decoded values from the
    URL's query string.
    """
    u = urlparse(url)
    base_url = u._replace(query=None).geturl()
    query = {k: v[0] for k, v in parse_qs(u.query).items()}

    return base_url, query


def _code_example(config):
    """Return a code example based on `config`."""

    dataset_type = config["dataset_type"]
    if not dataset_type:
        raise NoCodeExamples("Missing dataset type")

    if dataset_type not in dataset_types:
        raise NoCodeExamples(f"Unknown dataset type {dataset_type}")

    content_type = config["content_type"]
    if not content_type:
        raise NoCodeExamples("Missing content type")

    if content_type not in content_types:
        raise NoCodeExamples(f"Unknown content type {content_type}")

    context = {
        "dataset_id": config["dataset_id"],
        "version": config["version"],
        "dataset_type": dataset_type,
        "content_type": content_types[content_type],
        "access_rights": config["access_rights"],
        "api_base_url": None,
        "api_query": {},
        "requirements": [],
        "imports": [],
    }

    if config.get("api_url"):
        context["api_base_url"], context["api_query"] = _extract_query(
            config["api_url"]
        )

    code_example = template.render(**context)

    return {
        "content_type": config["content_type"],
        "code": format_str(isort.code(code_example), mode=FileMode()),
    }


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
        _code_example(
            {
                "dataset_id": dataset_id,
                "version": version_id,
                "dataset_type": distribution.get("distribution_type"),
                "content_type": distribution.get("content_type"),
                "access_rights": dataset["accessRights"],
                "api_url": distribution.get("api_url"),
            }
        )
        for distribution in distributions
    ]
