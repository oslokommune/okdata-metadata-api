import argparse
import os
import json
from datetime import datetime

# Must be done before repository import.
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"

from metadata.dataset.repository import DatasetRepository  # noqa
from metadata.version.repository import VersionRepository  # noqa
from metadata.edition.repository import EditionRepository  # noqa
from metadata.distribution.repository import DistributionRepository  # noqa


def print_output(
    deleted_distributions,
    deleted_editions,
    deleted_versions,
    deleted_datasets,
    output_dir_path=None,
):
    output = json.dumps(
        (
            {
                "deleted_distributions": deleted_distributions,
                "deleted_editions": deleted_editions,
                "deleted_versions": deleted_versions,
                "deleted_datasets": deleted_datasets,
            }
        ),
        indent=2,
    )
    if output_dir_path:
        write_output(output_dir_path, output)
    print(output)


def read_input(input_path):
    with open(input_path) as f:
        return f.read().splitlines()


def write_output(output_dir_path, output_json):
    dt_now_iso = datetime.utcnow().isoformat()
    with open(f"{output_dir_path}/delete_datasets_result-{dt_now_iso}.json", "w+") as f:
        f.write(output_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to line separated txt file with dataset ids to be deleted",
    )
    parser.add_argument(
        "--output",
        required=False,
        help="Optional. Path to directory where you want output to be written",
    )

    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    apply_changes = args.apply

    datasets_to_delete = read_input(args.input)

    dataset_repository = DatasetRepository()
    version_repository = VersionRepository()
    edition_repository = EditionRepository()
    distribution_repository = DistributionRepository()

    deleted_distributions = []
    deleted_editions = []
    deleted_versions = []
    deleted_datasets = []

    try:
        for dataset_id in datasets_to_delete:
            # Get version_ids that are to be deleted
            version_ids = [
                version["Id"]
                for version in version_repository.get_versions(
                    dataset_id, exclude_latest=False
                )
            ]

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

            # Get distributions that are to be deleted. Here we do not extract only the "Id" field,
            # since we need information about "distribution_type" in order to clean up in s3 afterwards.
            distributions = []
            for edition_id in edition_ids:
                _dataset_id, _version, _edition = edition_id.split("/")
                distributions.extend(
                    distribution_repository.get_distributions(
                        _dataset_id, _version, _edition
                    )
                )

            # Delete distributions and store in deleted_distributions
            for distribution in distributions:
                _dataset_id, _version, _edition, _distribution_id = distribution[
                    "Id"
                ].split("/")

                if apply_changes:
                    distribution_repository.delete_distribution(
                        _dataset_id, _version, _edition, _distribution_id
                    )

                deleted_distributions.append(
                    {
                        "Id": distribution["Id"],
                        "distribution_type": distribution["distribution_type"],
                        "filenames": distribution.get("filenames"),
                    }
                )

            # Delete editions and store in deleted_editions
            for edition_id in edition_ids:
                _dataset_id, _version, _edition = edition_id.split("/")

                if apply_changes:
                    edition_repository.delete_edition(_dataset_id, _version, _edition)

                deleted_editions.append(edition_id)

            # Delete versions and store in deleted_versions
            for version_id in version_ids:
                _dataset_id, _version = version_id.split("/")

                if apply_changes:
                    version_repository.delete_version(_dataset_id, _version)
                deleted_versions.append(version_id)

            # Delete dataset and store in deleted_datasets
            if apply_changes:
                dataset_repository.delete_dataset(dataset_id)
                print(f"Deleted: {dataset_id}")

            deleted_datasets.append(dataset_id)

    finally:
        print_output(
            deleted_distributions,
            deleted_editions,
            deleted_versions,
            deleted_datasets,
            args.output,
        )
