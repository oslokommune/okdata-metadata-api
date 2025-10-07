"""Script for setting parent_id on a list of datasets and migrating S3 data.

This script updates a list of dataset IDs to have a specified parent_id value
AND moves their S3 data to be nested under the parent dataset folder. It will only
work for parent datasets with source type 'none' and for those datasets that dont
already have a parent_id set.

S3 structure: {stage}/{confidentiality}/{dataset_id}/version={version}/edition={edition}/{filename}

When setting parent_id, S3 data is moved from:
  {stage}/{confidentiality}/child-dataset/version=1/...
to:
  {stage}/{confidentiality}/parent-dataset/child-dataset/version=1/...

Example usage:

# Set parent_id for datasets from a file (one dataset ID per line):
python -m scripts.set_parent_id \
  --env=dev \
  --parent_id=my-parent-dataset \
  --dataset-ids-file=dataset_ids.txt \
  --apply

# Set parent_id for specific datasets via command line:
python -m scripts.set_parent_id \
  --env=dev \
  --parent_id=my-parent-dataset \
  --dataset-ids dataset-1 dataset-2 dataset-3 \
  --apply

Note: The --apply flag is required to actually perform the updates.
"""

import argparse
import os
import sys
import boto3
from metadata.dataset.repository import DatasetRepository
from metadata.error import ValidationError
from metadata.common import BOTO_RESOURCE_COMMON_KWARGS, CONFIDENTIALITY_MAP, STAGES
from metadata.util import getenv

# Disable X-Ray
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"


def read_dataset_ids_from_file(filepath):
    """Read dataset IDs from a file, one per line, ignoring empty lines and comments."""
    dataset_ids = []
    try:
        with open(filepath, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    dataset_ids.append(line)
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{filepath}': {e}")
        sys.exit(1)

    return dataset_ids


def move_s3_data(
    s3_client,
    bucket,
    child_dataset_id,
    parent_dataset_id,
    child_access_rights,
    dry_run=True,
):
    confidentiality = CONFIDENTIALITY_MAP.get(child_access_rights)

    if not confidentiality:
        print(
            f"Warning: Unknown access rights '{child_access_rights}', skipping S3 migration"
        )
        return 0, 0

    total_moved = 0
    total_errors = 0

    for stage in STAGES:
        prefix = f"{stage}/{confidentiality}/{child_dataset_id}/"

        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    old_key = obj["Key"]

                    # Build new key by nesting child under parent
                    # FROM: {stage}/{confidentiality}/{child_dataset_id}/{rest}
                    # TO:   {stage}/{confidentiality}/{parent_dataset_id}/{child_dataset_id}/{rest}
                    parts = old_key.split(
                        "/", 3
                    )  # Split into [stage, confidentiality, dataset_id, rest]

                    if len(parts) < 4:
                        print(f"Warning: Unexpected S3 key format: {old_key}")
                        continue

                    # Nest child dataset under parent dataset
                    new_key = f"{parts[0]}/{parts[1]}/{parent_dataset_id}/{child_dataset_id}/{parts[3]}"

                    print(f"{'[DRY RUN] ' if dry_run else ''}Moving S3 object:")
                    print(f"  From: {old_key}")
                    print(f"  To:   {new_key}")

                    if not dry_run:
                        # Copy to new location
                        try:
                            copy_source = {"Bucket": bucket, "Key": old_key}
                            copy_response = s3_client.copy_object(
                                Bucket=bucket, CopySource=copy_source, Key=new_key
                            )

                            # Verify copy succeeded before deleting
                            if (
                                copy_response["ResponseMetadata"]["HTTPStatusCode"]
                                != 200
                            ):
                                print(
                                    f"ERROR copying S3 object (HTTP {copy_response['ResponseMetadata']['HTTPStatusCode']})"
                                )
                                total_errors += 1
                                continue

                        except Exception as e:
                            print(f"ERROR copying S3 object: {e}")
                            total_errors += 1
                            continue

                        # Only delete if copy succeeded
                        try:
                            s3_client.delete_object(Bucket=bucket, Key=old_key)
                            total_moved += 1

                        except Exception as e:
                            print(
                                f"WARNING: Copy succeeded but delete failed for {old_key}: {e}"
                            )
                            print("==Object exists at both old and new locations!==")
                            total_moved += (
                                1  # Still count as moved since copy succeeded
                            )

                    else:
                        total_moved += 1

        except Exception as e:
            print(f"ERROR listing S3 objects for stage '{stage}': {e}")
            total_errors += 1

    return total_moved, total_errors


def validate_parent_dataset(dataset_repository, parent_id):
    """Validate that the parent dataset exists and has source type 'none'."""
    print(f"\nValidating parent dataset: {parent_id}")
    try:
        parent_dataset = dataset_repository.get_dataset(parent_id)
        if not parent_dataset:
            print(f"Error: Parent dataset '{parent_id}' does not exist.")
            sys.exit(1)

        source_type = parent_dataset.get("source", {}).get("type")
        if source_type != "none":
            print(
                f"Warning: Parent dataset has source type '{source_type}', expected 'none'."
            )
            print("This may cause validation errors when creating child datasets.")
            print("Aborted.")
            sys.exit(1)
        else:
            print("Parent dataset exists with source type 'none'")
    except Exception as e:
        print(f"Error validating parent dataset: {e}")
        sys.exit(1)


def process_dataset(
    dataset_repository,
    s3_client,
    data_bucket,
    dataset_id,
    parent_id,
    apply_changes,
):
    """Process a single dataset: migrate S3 data and update metadata."""
    # Check if dataset exists
    dataset = dataset_repository.get_dataset(dataset_id)
    if not dataset:
        print(f"SKIP: Dataset '{dataset_id}' does not exist")
        return "skipped", 0, 0

    # Check current parent_id value
    current_parent_id = dataset.get("parent_id")

    if current_parent_id == parent_id:
        print(f"SKIP: Dataset '{dataset_id}' already has parent_id='{parent_id}'")
        return "skipped", 0, 0

    print(
        f"\n{'[DRY RUN] ' if not apply_changes else ''}Updating dataset: {dataset_id}"
    )
    print(
        f"Current parent_id: {current_parent_id if current_parent_id else '(not set)'}"
    )
    print(f"New parent_id: {parent_id}")

    # Move S3 data
    s3_moved = 0
    s3_errors = 0
    access_rights = dataset.get("accessRights")
    if access_rights:
        print(f"Migrating S3 data (accessRights: {access_rights})...")
        s3_moved, s3_errors = move_s3_data(
            s3_client,
            data_bucket,
            dataset_id,
            parent_id,
            access_rights,
            dry_run=not apply_changes,
        )

        if s3_moved > 0:
            print(
                f"S3: {s3_moved} objects {'would be moved' if not apply_changes else 'moved'}"
            )
        else:
            print("S3: No objects found to migrate")

        if s3_errors > 0:
            print(f"S3 ERRORS: {s3_errors} objects failed")
    else:
        print("Warning: No accessRights set, skipping S3 migration")

    # And update metadata
    if apply_changes:
        dataset_repository.patch_dataset(dataset_id, {"parent_id": parent_id})
        print("Updated successfully")
        return "success", s3_moved, s3_errors

    return "would_update", s3_moved, s3_errors


def main():
    parser = argparse.ArgumentParser(
        description="Set parent_id for a list of datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--env",
        required=True,
        choices=["dev", "prod"],
        help="Environment to run against",
    )
    parser.add_argument(
        "--parent_id", required=True, help="The parent dataset ID to set"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually apply the changes (default is dry-run)",
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--dataset-ids-file", help="Path to file containing dataset IDs (one per line)"
    )
    input_group.add_argument(
        "--dataset-ids", nargs="+", help="Space-separated list of dataset IDs"
    )

    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    dataset_repository = DatasetRepository()

    data_bucket = getenv("DATA_BUCKET_NAME")
    s3_client = boto3.client("s3", **BOTO_RESOURCE_COMMON_KWARGS)

    # Get the list of dataset IDs to update
    if args.dataset_ids_file:
        dataset_ids = read_dataset_ids_from_file(args.dataset_ids_file)
        print(f"Read {len(dataset_ids)} dataset IDs from {args.dataset_ids_file}")
    else:
        dataset_ids = args.dataset_ids
        print(f"Processing {len(dataset_ids)} dataset IDs from command line")

    # Validate parent dataset
    validate_parent_dataset(dataset_repository, args.parent_id)

    # Process each dataset
    success_count = 0
    error_count = 0
    skipped_count = 0
    total_s3_moved = 0
    total_s3_errors = 0

    print(f"\n{'=' * 60}")
    print(
        f"Mode: {'APPLYING CHANGES' if args.apply else 'DRY RUN (use --apply to make changes)'}"
    )
    print(f"Parent ID: {args.parent_id}")
    print(f"Datasets to update: {len(dataset_ids)}")
    print(f"S3 Bucket: {data_bucket}")
    print(f"{'=' * 60}\n")

    for dataset_id in dataset_ids:
        try:
            status, s3_moved, s3_errors = process_dataset(
                dataset_repository,
                s3_client,
                data_bucket,
                dataset_id,
                args.parent_id,
                args.apply,
            )

            total_s3_moved += s3_moved
            total_s3_errors += s3_errors

            if status == "success":
                success_count += 1
            elif status == "skipped":
                skipped_count += 1

        except ValidationError as e:
            print(f"VALIDATION ERROR: {e}")
            error_count += 1
        except Exception as e:
            print(f"ERROR: {e}")
            error_count += 1

    # Summary
    print(f"\n{'=' * 60}")
    print("Summary:")
    print(f"Total datasets: {len(dataset_ids)}")
    if args.apply:
        print(f"Successfully updated: {success_count}")
    else:
        print(f"Would be updated: {len(dataset_ids) - skipped_count - error_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Errors: {error_count}")
    print("\nS3 Migration:")
    if args.apply:
        print(f"S3 objects moved: {total_s3_moved}")
    else:
        print(f"S3 objects to move: {total_s3_moved}")
    print(f"S3 errors: {total_s3_errors}")
    print(f"{'=' * 60}")

    if not args.apply and (len(dataset_ids) - skipped_count - error_count) > 0:
        print("\nRun with --apply to actually perform the updates.")


if __name__ == "__main__":
    main()
