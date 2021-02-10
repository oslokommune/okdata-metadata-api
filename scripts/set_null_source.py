import os
from metadata.dataset.repository import DatasetRepository
from aws_xray_sdk.core import xray_recorder

xray_recorder.begin_segment("Set-file-source-script")

env = "dev"

# For all datasets checks for parent_id and adds parent_id to a list
# Sets source=null for all datasets with id=parant_id

if __name__ == "__main__":
    if env == "dev":
        os.environ["AWS_PROFILE"] = "okdata-dev"
    elif env == "prod":
        os.environ["AWS_PROFILE"] = "okdata-prod"
    else:
        raise Exception(f"Invalid env {env}")

    dataset_repository = DatasetRepository()

    all_datasets = dataset_repository.get_datasets()

    parent_dataset_ids = []

    for dataset in all_datasets:
        if dataset.get("parent_id"):
            parent_dataset_ids.append(dataset["parent_id"])

    for dataset_id in set(parent_dataset_ids):
        dataset_repository.patch_item(dataset_id, {"source": None})
