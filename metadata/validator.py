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
        except IOError as e:
            print(e)
            raise Exception(f"Missing schema for object {object_type}!")

    def validate(self, validation_object):
        return validate_object(
            instance=validation_object,
            schema=self.schema,
            format_checker=FormatChecker(),
        )
