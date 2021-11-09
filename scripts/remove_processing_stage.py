import argparse
import os
import time

from boto3.dynamodb.conditions import Key

# Must be done before repository import.
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"

from metadata.dataset.repository import DatasetRepository  # noqa


def query_all(table, **query):
    """Return every result from `table` by evaluating `query`."""
    res = table.query(**query)
    items = res["Items"]

    while "LastEvaluatedKey" in res:
        time.sleep(1)  # Let's be nice
        res = table.query(ExclusiveStartKey=res["LastEvaluatedKey"], **query)
        items.extend(res["Items"])

    return items


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    print("Starting migration process, scanning for Type=Dataset")
    repository = DatasetRepository()
    items = query_all(
        repository.metadata_table,
        IndexName="IdByTypeIndex",
        KeyConditionExpression=Key("Type").eq("Dataset"),
    )

    for item in items:
        dataset_id = item["Id"]

        print(f"Processing dataset-metadata.id={dataset_id}")

        if args.apply:
            print(f"\t\tNOT dry_run, removing processing_stage from {dataset_id}")
            repository.metadata_table.update_item(
                Key={"Id": id, "Type": "Dataset"},
                UpdateExpression="REMOVE processing_stage",
            )
