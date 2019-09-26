from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
from metadata import common
from aws_xray_sdk.core import patch

patch(["boto3"])

log = logging.getLogger()


class CommonRepository:
    def __init__(self, table, type):
        self.table = table
        self.type = type

    def get_item(self, id):
        key = {common.ID_COLUMN: id, common.TYPE_COLUMN: self.type}
        db_response = self.table.get_item(Key=key)

        if "Item" in db_response:
            item = db_response["Item"]

            # Set correct ID for 'latest' version/edition
            if "latest" in item:
                item["Id"] = item.pop("latest")

            return item

        log.info(f"Item {id} not found.")
        return None

    def get_items(self, parent_id=None):
        cond = Key(common.TYPE_COLUMN).eq(self.type)
        if parent_id:
            cond = cond & Key(common.ID_COLUMN).begins_with(f"{parent_id}/")

        db_response = self.table.query(
            IndexName="IdByTypeIndex", KeyConditionExpression=cond
        )
        items = db_response["Items"]

        # Remove 'latest' version/edition
        items = list(filter(lambda i: "latest" not in i, items))

        return items

    def create_item(
        self, id, content, parent_id=None, parent_type=None, update_on_exists=False
    ):
        if parent_id:
            parent_key = {common.ID_COLUMN: parent_id, common.TYPE_COLUMN: parent_type}
            db_response = self.table.get_item(Key=parent_key)
            if "Item" not in db_response:
                msg = f"Parent item with id {parent_id} does not exist"
                log.error(msg)
                raise KeyError(msg)

        content[common.ID_COLUMN] = id
        content[common.TYPE_COLUMN] = self.type

        try:
            if update_on_exists:
                db_response = self.table.put_item(Item=content)
            else:
                cond = "attribute_not_exists(Id) AND attribute_not_exists(#Type)"
                db_response = self.table.put_item(
                    Item=content,
                    ExpressionAttributeNames={"#Type": common.TYPE_COLUMN},
                    ConditionExpression=cond,
                )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ConditionalCheckFailedException":
                msg = f"Item with id {id} already exists"
                log.error(msg)
                raise ResourceConflict(msg, e)
            else:
                msg = e.response["Error"]["Message"]
                log.error(msg)
                raise ValueError(f"Error creating item ({error_code}): {msg}")

        http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

        if http_status == 200:
            return id
        else:
            msg = f"Error creating item ({http_status}): {db_response}"
            log.exception(msg)
            raise ValueError(msg)

    def update_item(self, id, content):
        old_item = self.get_item(id)
        if not old_item:
            raise KeyError(f"Item with id {id} does not exist")

        content[common.ID_COLUMN] = old_item[common.ID_COLUMN]
        content[common.TYPE_COLUMN] = self.type

        db_response = self.table.put_item(Item=content)

        http_status = db_response["ResponseMetadata"]["HTTPStatusCode"]

        if http_status == 200:
            return id
        else:
            msg = f"Error updating item ({http_status}): {db_response}"
            log.exception(msg)
            raise ValueError(msg)


class ResourceConflict(Exception):
    def __init__(self, msg, errors):
        super().__init__(msg)
        self.errors = errors
