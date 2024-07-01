from datetime import datetime
import dataclasses
import re

# fmt: off
# Regex for matching fields of log records in the S3 server access log format
# which are always present.
required_fields_pattern = '^' + ' '.join([
    r'(\S+)',                      # Bucket Owner
    r'(\S+)',                      # Bucket
    r'\[([\w:/]+\s[+\-]\d{4})\]',  # Time
    r'(\S+)',                      # Remote IP
    r'(\S+)',                      # Requester
    r'(\S+)',                      # Request ID
    r'(\S+)',                      # Operation
    r'(\S+)',                      # Key
    r'"?(-|[^"]*)"?',              # Request-URI
    r'(\S+)',                      # HTTP status
    r'(\S+)',                      # Error Code
    r'(\S+)',                      # Bytes Sent
    r'(\S+)',                      # Object Size
    r'(\S+)',                      # Total Time
    r'(\S+)',                      # Turn-Around Time
    r'"?(-|[^"]*)"?',              # Referer
    r'"?(-|[^"]*)"?',              # User-Agent
    r'(\S+)',                      # Version Id
])

# Amazon occasionally add new fields to the access log format, but these aren't
# necessarily present in our older log files, so optionally match these. This
# is designed to be future proof, as long as Amazon stick to their append-only
# policy for new fields.
optional_fields_pattern = ' ?'.join([
    r'(\S+)?',                     # Host Id
    r'(\S+)?',                     # Signature Version
    r'(\S+)?',                     # Cipher Suite
    r'(\S+)?',                     # Authentication Type
    r'(\S+)?',                     # Host Header
    r'(\S+)?',                     # TLS version
])
# fmt: on

pattern = re.compile(f"{required_fields_pattern} ?{optional_fields_pattern}.*")


class LogRecordParseError(RuntimeError):
    """Raised when an S3 access log line failed to parse."""

    pass


@dataclasses.dataclass
class LogRecord:
    """An S3 access log record."""

    bucket_owner: str
    bucket: str
    time: str
    remote_ip: str
    requester: str
    request_id: str
    operation: str
    key: str
    request_uri: str
    http_status: str
    error_code: str
    bytes_sent: str
    object_size: str
    total_time: str
    turn_around_time: str
    referer: str
    user_agent: str
    version_id: str
    host_id: str
    signature_version: str
    cipher_suite: str
    authentication_type: str
    host_header: str
    tls_version: str

    @staticmethod
    def _clean_field(field):
        return "" if field == "-" else field

    def datetime(self):
        """Return the log record time as a timezone aware `datetime` object."""
        return datetime.strptime(self.time, "%d/%b/%Y:%H:%M:%S %z")

    @classmethod
    def from_log_line(cls, log_line):
        """Parse `log_line` and return a correspodning `LogRecord` object.

        Raise `LogRecordParseError` if the log line doesn't parse.
        """
        if m := pattern.search(log_line):
            return cls(*map(cls._clean_field, m.groups()))

        # We expect our regex to match every log record.
        raise LogRecordParseError(f"Couldn't parse log record: {log_line}")
