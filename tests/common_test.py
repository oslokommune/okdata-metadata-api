from metadata import common
import json


class TestResponse:
    def test_response(self):
        statusCode = 201
        body = "one"
        response = common.response(statusCode, body)
        assert response["statusCode"] == statusCode
        assert isinstance(response["headers"], dict)
        assert response["headers"]["Access-Control-Allow-Origin"] == "*"
        assert response["body"] == json.dumps(body)

    def test_error_response(self):
        message = "a error occured"
        response = common.error_response(404, message)
        body = json.loads(response["body"])
        assert isinstance(body, list)
        assert isinstance(body[0], dict)
        assert body[0]["message"] == message
