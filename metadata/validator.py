import json
from pathlib import Path
from jsonschema import Draft7Validator, FormatChecker


class Validator:
    def __init__(self, object_type):
        self.path = Path(__file__).parent
        try:
            with open(f"{self.path.parent}/schema/{object_type}.json", "r") as f:
                schema = json.loads(f.read())
                self.validator = Draft7Validator(
                    schema=schema, format_checker=FormatChecker()
                )
        except IOError as e:
            print(e)
            raise Exception(f"Missing schema for object {object_type}!")

    def validate(self, validation_object):
        errors = []
        for e in self.validator.iter_errors(validation_object):
            prefix = ".".join(e.path) + ": " if e.path else ""
            errors.append(prefix + e.message)
        return errors
