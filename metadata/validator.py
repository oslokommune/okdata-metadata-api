import json
from pathlib import Path
from jsonschema import validate as validate_object
from jsonschema import FormatChecker


class Validator:
    def __init__(self, object_type):
        self.path = Path(__file__).parent
        try:
            with open(f"{self.path.parent}/schema/{object_type}.json", "r") as f:
                self.schema = json.loads(f.read())
        except Exception as e:
            print(e)

    def validate(self, validation_object):
        return validate_object(
            instance=validation_object,
            schema=self.schema,
            format_checker=FormatChecker(),
        )
