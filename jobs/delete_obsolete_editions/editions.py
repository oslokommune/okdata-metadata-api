from datetime import datetime, timedelta, timezone
from itertools import groupby

from metadata.edition.repository import EditionRepository

MIN_EDITION_AGE_DAYS = 90
MIN_EDITIONS_TO_KEEP = 3


def _edition_timestamp(edition):
    """Return the timestamp part of `edition`."""
    return datetime.fromisoformat(edition["edition"]).astimezone(timezone.utc)


def _old_enough_to_delete(edition):
    """Return true if `edition` is old enough to be automatically deleted."""
    return _edition_timestamp(edition) < datetime.now(timezone.utc) - timedelta(
        days=MIN_EDITION_AGE_DAYS
    )


def _edition_dataset(edition):
    """Return the dataset/version part of `edition`."""
    return "/".join(edition["Id"].split("/")[:2])


def _prunable_editions(editions):
    """Return the editions in `editions` that are prunable.

    "Prunable" being more than 90 days old, and past the third generation of
    editions.
    """
    return filter(
        _old_enough_to_delete,
        sorted(editions, key=_edition_timestamp, reverse=True)[MIN_EDITIONS_TO_KEEP:],
    )


def obsolete_editions():
    """Return a generator of obsolete editions."""
    edition_repository = EditionRepository()
    editions = edition_repository.get_editions()
    editions_by_dataset = groupby(
        sorted(editions, key=_edition_dataset), _edition_dataset
    )

    for dataset, dataset_editions in editions_by_dataset:
        for edition in _prunable_editions(dataset_editions):
            yield edition
