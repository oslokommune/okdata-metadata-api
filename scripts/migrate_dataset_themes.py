import argparse
import logging
import os

# Must be done before repository import.
os.environ["AWS_XRAY_SDK_ENABLED"] = "false"

from metadata.dataset.repository import DatasetRepository  # noqa

logger = logging.getLogger("migrate-dataset-themes")

THEMES = {
    "befolkning og samfunn": "population and society",
    "energi": "energy",
    "forvaltning og offentlig sektor": "government and public sector",
    "helse": "health",
    "internasjonale temaer": "international issues",
    "jordbruk, fiskeri, skogbruk og mat": "agriculture, fisheries, forestry and food",
    "justis, rettsystem og allmenn sikkerhet": "justice, legal system and public safety",
    "miljø": "environment",
    "økonomi og finans": "economy and finance",
    "regioner og byer": "regions and cities",
    "transport": "transport",
    "utdanning, kultur og sport": "education, culture and sport",
    "vitenskap og teknologi": "science and technology",
}


class UnknownThemeException(Exception):
    pass


def translate_themes(themes):
    if not isinstance(themes, list):
        themes = [themes]
    for theme in themes:
        theme = theme.lower()
        if theme in THEMES.values():
            yield theme
        elif theme in THEMES:
            yield THEMES[theme]
        else:
            raise UnknownThemeException(f'Unknown theme "{theme}"')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("-e", "--env", required=True, choices=["dev", "prod"])
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--log-level", default="INFO", choices=list(logging._nameToLevel.keys())
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.getLevelName(args.log_level),
        format="%(name)s - %(levelname)s - %(message)s",
    )

    os.environ["AWS_PROFILE"] = f"okdata-{args.env}"

    dataset_repository = DatasetRepository()
    datasets = dataset_repository.get_datasets()

    logger.info(f"Got {len(datasets)} datasets")

    for dataset in datasets:
        dataset_id = dataset["Id"]

        if "theme" not in dataset:
            logger.debug(f"No theme set for dataset {dataset_id}")
            continue

        try:
            updated_themes = list(translate_themes(dataset["theme"]))
        except UnknownThemeException as e:
            logger.warning(f"Skipping dataset {dataset_id}: {e}")
            continue

        if updated_themes == dataset["theme"]:
            logger.debug(
                "Skipping dataset {}: No updates necessary ({})".format(
                    dataset_id, dataset["theme"]
                )
            )
            continue

        if args.apply:
            dataset_repository.patch_dataset(dataset_id, {"theme": updated_themes})

        logger.info(
            "{} {}: {} => {}".format(
                "Should update" if not args.apply else "Updated",
                dataset_id,
                dataset["theme"],
                updated_themes,
            )
        )
