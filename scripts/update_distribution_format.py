import argparse
import os
import time

from boto3.dynamodb.conditions import Attr, Key

os.environ["AWS_XRAY_SDK_ENABLED"] = "false"  # Must be done before repository imports

from metadata.CommonRepository import MissingParentError
from metadata.distribution.repository import DistributionRepository  # noqa


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

    distribution_repository = DistributionRepository()

    items = query_all(
        distribution_repository.metadata_table,
        IndexName="IdByTypeIndex",
        KeyConditionExpression=Key("Type").eq("Distribution"),
        FilterExpression=Attr("filename").exists(),
    )

    # Mapping of each dataset URI ("<dataset>/<version>/<edition>") to a list
    # of its distribution entries.
    dataset_uri_map = {}

    for item in items:
        dataset, version, edition, *_ = item["Id"].split("/")
        key = f"{dataset}/{version}/{edition}"
        dataset_uri_map.setdefault(key, []).append(item)

    for dataset_uri, entries in dataset_uri_map.items():
        dataset, version, edition = dataset_uri.split("/")
        content = {
            "distribution_type": "file",
            "filenames": sorted(list({entry["filename"] for entry in entries})),
        }

        if args.apply:
            try:
                distribution_id = distribution_repository.create_distribution(
                    dataset, version, edition, content
                )
                print(f"Created distribution {distribution_id}")
            except MissingParentError:
                print(f"Edition not found: {dataset_uri}, skipping creation")
        else:
            print("Should create:")
            print(f"  {content} for {dataset}/{version}/{edition}")
            print("Should delete:")

        for entry in entries:
            if args.apply:
                distribution_repository.metadata_table.delete_item(
                    Key={
                        "Id": entry["Id"],
                        "Type": "Distribution",
                    }
                )
                print(f"  Deleted distribution {entry['Id']}")
            else:
                print(f"  {entry}")

    print(
        "Number of distributions {}: {}".format(
            "deleted" if args.apply else "to delete",
            len(items),
        )
    )
    print(
        "Number of distributions {}: {}".format(
            "created" if args.apply else "to create",
            len(dataset_uri_map),
        )
    )
