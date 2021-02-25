import argparse
import os
import time

from boto3.dynamodb.conditions import Attr, Key

# Must be done before repository import.
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"

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
    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    distribution_repo = DistributionRepository()

    items = query_all(
        distribution_repo.metadata_table,
        IndexName="IdByTypeIndex",
        KeyConditionExpression=Key("Type").eq("Distribution"),
        FilterExpression=Attr("distribution_type").not_exists(),
    )

    for item in items:
        if "filename" in item or "filenames" in item:
            if "api_url" in item:
                raise ValueError(f"{item} has both `filename(s)` and `api_url` set!")
            distribution_type = "file"
        elif "api_url" in item:
            distribution_type = "api"
        else:
            raise ValueError(f"{item} has neither `filename(s)` nor `api_url` set!")

        distribution_repo.patch_item(
            item["Id"], {"distribution_type": distribution_type}
        )
        print(f"Updated {item['Id']} to '{distribution_type}'")
