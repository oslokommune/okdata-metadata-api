import argparse
import json
import logging
import os
import sys
from datetime import datetime

import boto3

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
    deleted_objects,
    output_dir_path=None,
):
    output = json.dumps(
        (
            {
                "deleted_distributions": deleted_distributions,
                "deleted_editions": deleted_editions,
                "deleted_versions": deleted_versions,
                "deleted_objects": deleted_objects,
            }
        ),
        indent=2,
    )
    if output_dir_path:
        write_output(output_dir_path, dataset_id, output)
    print(output)


def write_output(output_dir_path, dataset_id, output_json):
    dt_now_iso = datetime.utcnow().strftime("%Y-%m-%d-%H.%M.%S")
    output_file_path = os.path.join(
        output_dir_path, f"{dt_now_iso}_deleted_dataset_{dataset_id}.json"
    )
    logger.info(f"Writing output to {output_file_path}")
    with open(output_file_path, "w+") as f:
        f.write(output_json)


def find_s3_objects(bucket, dataset_id):
    s3 = boto3.client("s3", region_name="eu-west-1")

    stages = [
        cp["Prefix"]
        for cp in s3.list_objects_v2(
            Bucket=bucket,
            Delimiter="/",
        )["CommonPrefixes"]
    ]

    objects = []

    paginator = s3.get_paginator("list_objects_v2")

    for stage in stages:
        for confidentiality in ["green", "yellow", "red"]:
            prefix = f"{stage}{confidentiality}/{dataset_id}"
            logger.debug(f"Looking for data in {prefix}")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    objects.append(obj["Key"])
    return objects


def delete_s3_objects(bucket, objects):
    objects = [{"Key": key} for key in objects]
    s3 = boto3.client("s3", region_name="eu-west-1")
    response = s3.delete_objects(
        Bucket=bucket,
        Delete={"Objects": objects},
    )
    return [obj["Key"] for obj in response.get("Deleted", [])]


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
        "--delete-s3-data",
        default=False,
        help="Optional. Identify and delete any dataset data from S3",
        action="store_true",
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
    s3_bucket = f"ok-origo-dataplatform-{args.env}"

    dataset_id = args.dataset_id
    delete_s3_data = args.delete_s3_data
    apply_changes = args.apply

    dataset_repository = DatasetRepository()
    version_repository = VersionRepository()
    edition_repository = EditionRepository()
    distribution_repository = DistributionRepository()

    deleted_distributions = []
    deleted_editions = []
    deleted_versions = []
    deleted_datasets = []
    deleted_objects = []

    try:
        logger.info(f"Preparing to delete dataset {dataset_id}")

        # Get dataset metadata
        dataset = dataset_repository.get_dataset(dataset_id)

        if not dataset:
            sys.exit(f"Aborted: Dataset {dataset_id} does not exist")

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

        logger.info(f"Found {len(distributions)} distributions")

        # Find dataset data in S3
        s3_objects = []
        if delete_s3_data:
            s3_objects = find_s3_objects(s3_bucket, dataset_id)

            logger.info(f"Found {len(s3_objects)} S3 objects in {s3_bucket}")

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

        deleted_datasets.append(dataset_id)

        # Delete S3 data
        if apply_changes and len(s3_objects) > 0:
            deleted_objects = delete_s3_objects(s3_bucket, s3_objects)
        else:
            deleted_objects = s3_objects

    finally:
        print_output(
            dataset_id,
            deleted_distributions,
            deleted_editions,
            deleted_versions,
            deleted_objects,
            args.output,
        )
