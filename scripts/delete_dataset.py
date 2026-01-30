import argparse
import json
import logging
import os
from datetime import datetime, timezone

# Must be done before repository import.
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"

from metadata.dataset.repository import DatasetRepository  # noqa
from metadata.version.repository import VersionRepository  # noqa
from metadata.edition.repository import EditionRepository  # noqa
from metadata.distribution.repository import DistributionRepository  # noqa

logger = logging.getLogger("delete_dataset")


def print_output(
    dataset_id,
    deleted_distributions,
    deleted_editions,
    deleted_versions,
    output_dir_path=None,
):
    output = json.dumps(
        (
            {
                "deleted_distributions": deleted_distributions,
                "deleted_editions": deleted_editions,
                "deleted_versions": deleted_versions,
            }
        ),
        indent=2,
    )
    if output_dir_path:
        write_output(output_dir_path, dataset_id, output)
    print(output)


def write_output(output_dir_path, dataset_id, output_json):
    dt_now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H.%M.%S")
    output_file_path = os.path.join(
        output_dir_path, f"{dt_now_iso}_deleted_dataset_{dataset_id}.json"
    )
    logger.info(f"Writing output to {output_file_path}")
    with open(output_file_path, "w+") as f:
        f.write(output_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--dataset-id",
        required=True,
        help="Id for dataset to be deleted",
    )
    parser.add_argument(
        "--output",
        required=False,
        help="Optional. Path to directory where you want output to be written",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=list(logging._nameToLevel.keys()),
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.getLevelName(args.log_level))

    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"
    os.environ["AWS_REGION"] = "eu-west-1"
    os.environ["DATA_BUCKET_NAME"] = f"ok-origo-dataplatform-{args.env}"

    dataset_id = args.dataset_id
    apply_changes = args.apply

    dataset_repository = DatasetRepository()
    version_repository = VersionRepository()
    edition_repository = EditionRepository()
    distribution_repository = DistributionRepository()

    deleted_distributions = []
    deleted_editions = []
    deleted_versions = []
    deleted_datasets = []

    try:
        logger.info(f"Preparing to delete dataset {dataset_id}")

        # Get version_ids that are to be deleted
        version_ids = [
            version["Id"]
            for version in version_repository.get_versions(
                dataset_id, exclude_latest=False
            )
        ]

        logger.info(f"Found {len(version_ids)} versions")

        # Get edition_ids that are to be deleted
        edition_ids = []
        for version_id in version_ids:
            _dataset_id, _version = version_id.split("/")
            edition_ids.extend(
                [
                    edition["Id"]
                    for edition in edition_repository.get_editions(
                        _dataset_id, _version, exclude_latest=False
                    )
                ]
            )

        logger.info(f"Found {len(edition_ids)} editions")

        # Get distributions that are to be deleted
        distributions = []
        for edition_id in edition_ids:
            _dataset_id, _version, _edition = edition_id.split("/")
            distributions.extend(
                distribution_repository.get_distributions(
                    _dataset_id, _version, _edition
                )
            )

        logger.info(f"Found {len(distributions)} distributions")

        # Delete distributions and store in deleted_distributions
        for distribution in distributions:
            if apply_changes:
                distribution_repository.delete_item(distribution["Id"])

            deleted_distributions.append(
                {
                    "Id": distribution["Id"],
                    "distribution_type": distribution["distribution_type"],
                    "filenames": distribution.get("filenames"),
                }
            )

        # Delete editions and store in deleted_editions
        for edition_id in edition_ids:
            if apply_changes:
                edition_repository.delete_item(edition_id)

            deleted_editions.append(edition_id)

        # Delete versions and store in deleted_versions
        for version_id in version_ids:
            if apply_changes:
                version_repository.delete_item(version_id)

            deleted_versions.append(version_id)

        # Delete dataset and store in deleted_datasets
        if apply_changes:
            dataset_repository.delete_item(dataset_id)

        deleted_datasets.append(dataset_id)

    finally:
        print_output(
            dataset_id,
            deleted_distributions,
            deleted_editions,
            deleted_versions,
            args.output,
        )
