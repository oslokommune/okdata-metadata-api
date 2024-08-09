import re
from dataclasses import dataclass
from urllib.parse import unquote

STAGES = ["raw", "intermediate", "processed"]
CONFIDENTIALITIES = ["green", "yellow", "red"]


@dataclass
class DatasetEntry:
    """An S3 object entry that looks like it belongs to a dataset."""

    stage: str
    confidentiality: str
    dataset_id: str
    version: str
    edition: str
    filepath: str

    @classmethod
    def from_s3_key(cls, key):
        """Return a `DatasetEntry` object corresponding to `key`.

        If `key` doesn't look like it belongs to a dataset, return `None`.
        """
        if isinstance(key, str):
            # Amazon URL-encodes the key twice for unknown reasons, so decode
            # it twice.
            key_unquoted = unquote(unquote(key))

            # fmt: off
            pattern = re.compile("/".join([
                r"(?P<stage>[^/]+)",            # Stage
                r"(?P<confidentiality>[^/]+)",  # Confidentiality
                r"(?P<dataset>\S+)",            # Dataset
                r"version=(?P<version>[^/]+)",  # Version
                r"edition=(?P<edition>[^/]+)",  # Edition
                r"(?P<filepath>.+)$",           # Filepath
            ]))
            # fmt: on

            if match := pattern.search(key_unquoted):
                stage = match.group("stage")
                confidentiality = match.group("confidentiality")
                dataset = match.group("dataset").split("/")[-1]
                version = match.group("version")
                edition = match.group("edition")
                filepath = match.group("filepath")

                if stage in STAGES and confidentiality in CONFIDENTIALITIES:
                    return cls(
                        stage, confidentiality, dataset, version, edition, filepath
                    )

        return None
