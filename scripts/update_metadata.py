"""Script for mass-updating dataset metadata.

Supports filtering and nested attribute lookup by using dotted paths.

For example, the following invocation will update every dataset in dev where
`contactPoint.email` matches "bydelsfakta@byr.oslo.kommune.no" to
"oslostatistikken@byr.oslo.kommune.no":

python -m scripts.update_metadata \
  --env=dev \
  --key=contactPoint.email \
  --from-value=bydelsfakta@byr.oslo.kommune.no \
  --to-value=oslostatistikken@byr.oslo.kommune.no \
  --apply
"""

import argparse
import os
import time
from copy import deepcopy
from functools import reduce

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


def lookup_nested_dict(dic, path):
    """Lookup the nested dictionary `dic` on the iterable `path`."""
    return reduce(lambda d, key: d.get(key, {}), path, dic)


def update_nested_dict(dic, path, value):
    """Update the nested dictionary `dic` on the iterable `path` with `value`."""
    if len(path) > 1:
        update_nested_dict(dic[path[0]], path[1:], value)
    else:
        dic[path[0]] = value


def filter_datasets_to_update(datasets, key, from_value):
    """Return datasets in `datasets` with `from_value` in `key`.

    `key` supports nested lookups with each part separated by `.`.
    """
    path = key.split(".")

    return [
        dataset
        for dataset in datasets
        if lookup_nested_dict(dataset, path) == from_value
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--key", required=True, help="Dataset attribute to update")
    parser.add_argument(
        "--from-value", help="Update only datasets with this existing value"
    )
    parser.add_argument("--to-value", required=True, help="New attribute value")
    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    repository = DatasetRepository()
    datasets = query_all(
        repository.metadata_table,
        IndexName="IdByTypeIndex",
        KeyConditionExpression=Key("Type").eq("Dataset"),
    )

    if args.from_value:
        datasets = filter_datasets_to_update(datasets, args.key, args.from_value)

    attribute, *key_rest = args.key.split(".")
    repository = DatasetRepository()

    for dataset in datasets:
        print("\n{}Updating:".format("" if args.apply else "[DRY RUN] "))
        print(f"    dataset: {dataset['Id']}")
        print(f"  attribute: {attribute}")

        old_value = dataset[attribute]
        if key_rest:
            new_value = deepcopy(old_value)
            update_nested_dict(new_value, key_rest, args.to_value)
        else:
            new_value = args.to_value

        print(f" from value: {old_value}")
        print(f"   to value: {new_value}")

        if args.apply:
            repository.patch_dataset(dataset["Id"], {attribute: new_value})
