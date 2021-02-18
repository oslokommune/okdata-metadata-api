import argparse
import os
import time
from metadata.dataset.repository import DatasetRepository
from aws_xray_sdk.core import xray_recorder

xray_recorder.begin_segment("Set-file-source-script")

# At this point in time source.type for all event datasets are set to 'event'
# and source.type for all database sourced datasets (bym-geodata) are set to 'database'
# This means that all datasets where source is not set can be set to source.type='file'

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    dataset_repository = DatasetRepository()

    all_datasets = dataset_repository.get_datasets()

    for dataset in all_datasets:
        if "source" not in dataset:
            dataset_id = dataset["Id"]
            dataset_repository.patch_item(dataset_id, {"source": {"type": "file"}})
            print(f"Updated: {dataset_id}")
            time.sleep(0.5)
