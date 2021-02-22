import argparse
import os
import time
from datetime import datetime

# Not part of metadata-api's requirements; please install manually.
import pytz

# Must be done before repository imports.
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"

from metadata.distribution.repository import DistributionRepository  # noqa
from metadata.edition.repository import EditionRepository  # noqa
from metadata.version.repository import VersionRepository  # noqa

DATASETS = {
    "bym-samferdsel-elsparkesykler": {
        "feature_name": "bym:elsparkesykler",
    },
    "bym-samferdsel-neringsparkering": {
        "feature_name": "bym:næringsparkering",
    },
    "bym-str-isycase-area": {
        "feature_name": "bym:isycase_area",
    },
    "bym-str-isycase-linje": {
        "feature_name": "bym:isycase_linje",
    },
    "bym-str-isycase-punkt": {
        "feature_name": "bym:isycase_punkt",
    },
}


def query_all(table, **query):
    """Return every result from `table` by evaluating `query`."""
    res = table.query(**query)
    items = res["Items"]

    while "LastEvaluatedKey" in res:
        time.sleep(1)  # Let's be nice
        res = table.query(ExclusiveStartKey=res["LastEvaluatedKey"], **query)
        items.extend(res["Items"])

    return items


def api_url(env, dataset_id, content_type):
    try:
        output_format = {
            "application/geo+json": "application/json",
            "text/csv": "csv",
        }[content_type]
    except KeyError:
        raise ValueError(f"Unknown content_type: {content_type}")

    return "https://geoserver.data{}.oslo.systems/geoserver/bym/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={}&outputFormat={}".format(
        "" if env == "prod" else "-dev", dataset_id, output_format
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", required=True, choices=["dev", "prod"])
    args = parser.parse_args()

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    distribution_repository = DistributionRepository()
    edition_repository = EditionRepository()
    version_repository = VersionRepository()

    now = pytz.timezone("Europe/Oslo").localize(datetime.now())

    for dataset_id in DATASETS:
        version = version_repository.get_version(dataset_id, "latest")["version"]

        if edition_repository.edition_exists(dataset_id, version, "latest"):
            print(f"Edition exists for {dataset_id}, skipping")
        else:
            edition_id = edition_repository.create_edition(
                dataset_id,
                version,
                {
                    "edition": now.isoformat(timespec="seconds"),
                    "startTime": now.strftime("%Y-%m-%d"),
                },
            )

            print(f"Created edition {edition_id}")

    for dataset_id in DATASETS:
        version = version_repository.get_version(dataset_id, "latest")["version"]
        edition = edition_repository.get_edition(dataset_id, version, "latest")[
            "Id"
        ].split("/")[-1]

        if distribution_repository.get_distributions(dataset_id, version, edition):
            print(f"Distribution exists for {dataset_id}, skipping")
        else:
            for content_type in ["application/geo+json", "text/csv"]:
                distribution_id = distribution_repository.create_distribution(
                    dataset_id,
                    version,
                    edition,
                    {
                        "distribution_type": "api",
                        "content_type": content_type,
                        "api_url": api_url(
                            args.env, DATASETS[dataset_id]["feature_name"], content_type
                        ),
                    },
                )

                print(f"Created distribution {distribution_id}")
