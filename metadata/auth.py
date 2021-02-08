import os
import requests
from keycloak import KeycloakOpenID

from metadata.dataset.repository import DatasetRepository
from metadata.common import error_response


class Auth:
    def __init__(self):
        self.AUTHORIZER_API = os.environ["AUTHORIZER_API"]
        self.KEYCLOAK_SERVER = "{}/auth/".format(os.environ["KEYCLOAK_SERVER"])
        self.KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "api-catalog")
        self.CLIENT_ID = os.environ["CLIENT_ID"]
        self.CLIENT_SECRET = os.environ["CLIENT_SECRET"]

    def service_client_authorization_header(self):
        client = KeycloakOpenID(
            server_url=self.KEYCLOAK_SERVER,
            realm_name=self.KEYCLOAK_REALM,
            client_id=self.CLIENT_ID,
            client_secret_key=self.CLIENT_SECRET,
        )
        response = client.token(grant_type=["client_credentials"])
        access_token = f"{response['token_type']} {response['access_token']}"
        return {"Authorization": access_token}

    def is_dataset_owner(self, token, dataset_id):
        result = requests.get(
            f"{self.AUTHORIZER_API}/{dataset_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        result.raise_for_status()
        data = result.json()
        return data.get("access", False)


def check_auth(func):
    def wrapper(event, *args, **kwargs):
        path_parameters = event["pathParameters"]
        dataset_id = path_parameters["dataset-id"]

        if DatasetRepository().get_dataset(dataset_id) is None:
            message = f"Dataset {dataset_id} does not exist"
            return error_response(404, message)

        auth_header = event["headers"].get("Authorization")
        if not auth_header:
            message = "Authorization header missing"
            return error_response(403, message)

        bearer_token = auth_header.split(" ")[-1]
        if not Auth().is_dataset_owner(bearer_token, dataset_id):
            message = f"You are not authorized to access dataset {dataset_id}"
            return error_response(403, message)

        return func(event, *args, **kwargs)

    return wrapper
