import os
import requests
import logging
from simplejson.errors import JSONDecodeError
from aws_xray_sdk.core import patch

patch(["requests"])

log = logging.getLogger()
log.setLevel(logging.INFO)

AUTHORIZER_API = os.environ["AUTHORIZER_API"]


class SimpleAuth:
    def __init__(self, event):
        self.event = event

    def is_owner(self, dataset_id):
        header = None

        try:
            header = self.event["headers"]["Authorization"]
        except KeyError as e:
            log.exception(f"Authorization header KeyError: {e}")
            return False

        return is_owner(header, dataset_id)


def is_owner(authorization_header, dataset_id):
    r = requests.get(
        f"{AUTHORIZER_API}/{dataset_id}",
        headers={"Authorization": authorization_header},
    )

    try:
        data = r.json()
        return "access" in data and data["access"]
    except JSONDecodeError as e:
        log.exception(f"Authorization JSON decode failure: {e}")

    return False
