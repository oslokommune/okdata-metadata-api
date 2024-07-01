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
    edition_path: str
    filename: str

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
                r"(?P<edition_path>\S+)",       # Edition path
                r"(?P<filename>.+)$",           # Filename
            ]))
            # fmt: on

            match = pattern.search(key_unquoted)
            if match:
                return cls(
                    match.group("stage"),
                    match.group("confidentiality"),
                    match.group("dataset").split("/")[-1],
                    match.group("version"),
                    match.group("edition_path"),
                    match.group("filename"),
                )

        return None

    def is_valid(self):
        return self.stage in STAGES and self.confidentiality in CONFIDENTIALITIES
