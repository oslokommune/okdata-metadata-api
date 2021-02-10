import argparse
import os
import time

from aws_xray_sdk.core import xray_recorder
from metadata.dataset.repository import DatasetRepository

xray_recorder.begin_segment("Set-dataset-state-script")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    dataset_repository = DatasetRepository()

    for dataset in dataset_repository.get_datasets():
        if "state" not in dataset:
            dataset_id = dataset["Id"]
            print(f"Updating dataset '{dataset_id}'")
            dataset_repository.patch_item(dataset_id, {"state": "active"})
            time.sleep(0.5)
