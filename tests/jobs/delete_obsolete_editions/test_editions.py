from unittest.mock import patch
from datetime import datetime, timezone

from freezegun import freeze_time

from jobs.delete_obsolete_editions.editions import (
    _edition_dataset,
    _edition_timestamp,
    _old_enough_to_delete,
    _prunable_editions,
    obsolete_editions,
)


def test_edition_timestamp(editions):
    assert _edition_timestamp(editions[0]) == datetime(
        2023, 1, 1, 8, 0, 0, 0, timezone.utc
    )


@freeze_time("2023-04-07")
def test_old_enough_to_delete(editions):
    assert list(map(_old_enough_to_delete, editions)) == [True] * 2 + [False] * 3


def test_edition_dataset(editions):
    assert _edition_dataset(editions[0]) == "foo/1"


@freeze_time("2023-04-03")
def test_prunable_editions(editions):
    assert len(list(_prunable_editions(editions))) == 1


@freeze_time("2023-04-03")
@patch("jobs.delete_obsolete_editions.editions.EditionRepository")
def test_obsolete_editions_age(edition_repository, editions):
    edition_repository.return_value.get_editions.return_value = editions

    assert len(list(obsolete_editions())) == 1


@freeze_time("2024-01-01")
@patch("jobs.delete_obsolete_editions.editions.EditionRepository")
def test_obsolete_editions_number(edition_repository, editions):
    edition_repository.return_value.get_editions.return_value = editions

    assert len(list(obsolete_editions())) == 2
