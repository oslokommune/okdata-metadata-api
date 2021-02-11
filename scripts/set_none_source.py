import argparse
import os
from metadata.dataset.repository import DatasetRepository
from aws_xray_sdk.core import xray_recorder

xray_recorder.begin_segment("Set-file-source-script")

# For all datasets checks for parent_id and adds parent_id to a list
# Sets source=null for all datasets with id=parant_id

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    dataset_repository = DatasetRepository()

    all_datasets = dataset_repository.get_datasets()

    parent_dataset_ids = []

    for dataset in all_datasets:
        if dataset.get("parent_id"):
            parent_dataset_ids.append(dataset["parent_id"])

    for dataset_id in set(parent_dataset_ids):
        try:
            dataset_repository.patch_item(dataset_id, {"source": {"type": "none"}})
        except KeyError as e:
            print(f"Failed for {dataset_id}. Reason:")
            print(str(e))
        print(f"Updated {dataset_id}")
