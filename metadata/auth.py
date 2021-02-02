import os
import requests
from keycloak import KeycloakOpenID

AUTHORIZER_API = os.environ["AUTHORIZER_API"]
KEYCLOAK_SERVER = "{}/auth/".format(os.environ["KEYCLOAK_SERVER"])
KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "api-catalog")
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]


class Auth:
    def __init__(self):
        self.AUTHORIZER_API = os.environ["AUTHORIZER_API"]
        self.KEYCLOAK_SERVER = "{}/auth/".format(os.environ["KEYCLOAK_SERVER"])
        self.KEYCLOAK_REALM = os.environ.get("KEYCLOAK_REALM", "api-catalog")
        self.CLIENT_ID = os.environ["CLIENT_ID"]
        self.CLIENT_SECRET = os.environ["CLIENT_SECRET"]

    def service_client_authorization_header(self):
        client = KeycloakOpenID(
            server_url=KEYCLOAK_SERVER,
            realm_name=KEYCLOAK_REALM,
            client_id=CLIENT_ID,
            client_secret_key=CLIENT_SECRET,
        )
        response = client.token(grant_type=["client_credentials"])
        access_token = f"{response['token_type']} {response['access_token']}"
        return {"Authorization": access_token}

    def is_dataset_owner(self, token, dataset_id):
        result = requests.get(
            f"{AUTHORIZER_API}/{dataset_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        result.raise_for_status()
        data = result.json()
        return data.get("access", False)


def is_dataset_owner(token, dataset_id):
    result = requests.get(
        f"{AUTHORIZER_API}/{dataset_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    result.raise_for_status()
    data = result.json()
    return data.get("access", False)
