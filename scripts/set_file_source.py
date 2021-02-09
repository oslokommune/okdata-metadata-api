import os
from metadata.dataset.repository import DatasetRepository
from aws_xray_sdk.core import xray_recorder

xray_recorder.begin_segment("Set-file-source-script")

env = "dev"

# At this point in time source.type for all event datasets are set to 'event'
# and source.type for all database sourced datasets (bym-geodata) are set to 'database'
# This means that all datasets where source is not set can be set to source.type='file'

if __name__ == "__main__":
    if env == "dev":
        os.environ["AWS_PROFILE"] = "okdata-dev"
    elif env == "prod":
        os.environ["AWS_PROFILE"] = "okdata-prod"
    else:
        raise Exception(f"Invalid env {env}")

    dataset_repository = DatasetRepository()

    all_datasets = dataset_repository.get_datasets()

    for dataset in all_datasets:
        if "source" not in dataset:
            dataset_id = dataset["Id"]
            dataset_repository.patch_item(dataset_id, {"source": {"type": "file"}})
