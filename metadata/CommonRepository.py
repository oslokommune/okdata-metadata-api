from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging

from metadata import common
from metadata.error import ResourceConflict, ValidationError
from okdata.aws.logging import log_add, log_duration

from aws_xray_sdk.core import patch_all

patch_all()

log = logging.getLogger()


class CommonRepository:
    def __init__(self, table, type):
        self.table = table
        self.type = type

    def get_item(self, item_id, consistent_read=False):
        log_add(dynamodb_item_id=item_id, dynamodb_item_type=self.type)
        key = {common.ID_COLUMN: item_id, common.TYPE_COLUMN: self.type}

        db_response = log_duration(
            lambda: self.table.get_item(Key=key, ConsistentRead=consistent_read),
            "dynamodb_duration_ms",
        )

        status_code = db_response["ResponseMetadata"]["HTTPStatusCode"]
        log_add(dynamodb_status_code=status_code)

        if "Item" not in db_response:
            log.info(f"Item {item_id} not found.")
            log_add(dynamodb_num_items=0)
            return None

        log_add(dynamodb_num_items=1)
        item = db_response["Item"]

        # Set correct ID for 'latest' version/edition
        is_latest = "latest" in item
        log_add(dynamodb_item_is_latest=is_latest)
        if is_latest:
            item["Id"] = item.pop("latest")

        return item

    def get_items(self, parent_id=None):
        log_add(dynamodb_item_type=self.type)
        cond = Key(common.TYPE_COLUMN).eq(self.type)
        extra_query_args = {}

        if parent_id:
            log_add(dynamodb_parent_id=parent_id)
            if self.type == "Dataset":
                extra_query_args["FilterExpression"] = Key("parent_id").eq(parent_id)
            else:
                cond = cond & Key(common.ID_COLUMN).begins_with(f"{parent_id}/")

        db_response = log_duration(
            lambda: self.table.query(
                IndexName="IdByTypeIndex",
                KeyConditionExpression=cond,
                **extra_query_args,
            ),
            "dynamodb_duration_ms",
        )

        status_code = db_response["ResponseMetadata"]["HTTPStatusCode"]
        log_add(dynamodb_status_code=status_code)

        items = db_response["Items"]
        log_add(dynamodb_num_items=len(items))

        # Remove 'latest' version/edition
        items = list(filter(lambda i: "latest" not in i, items))

        return items

    def create_item(
        self, item_id, content, parent_id=None, parent_type=None, update_on_exists=False
    ):
        """Add `content` to `self.table` under the key `item_id`.

        Return the inserted key on success.

        When `parent_id` is given, perform an additional check that an entry
        exists with the given ID and type `parent_type`.

        When `update_on_exists` is true, any existing entry will be updated
        with the new content. Otherwise it's required that an entry with the ID
        doesn't already exist.
        """
        log_add(dynamodb_item_id=item_id, dynamodb_item_type=self.type)
        if parent_id:
            log_add(dynamodb_parent_id=parent_id)
            parent_key = {common.ID_COLUMN: parent_id, common.TYPE_COLUMN: parent_type}
            db_response = self.table.get_item(Key=parent_key)
            parent_exists = "Item" in db_response
            log_add(dynamodb_parent_exists=parent_exists)
            if not parent_exists:
                msg = f"Parent item with id {parent_id} does not exist"
                log.error(msg)
                raise KeyError(msg)

        content[common.ID_COLUMN] = item_id
        content[common.TYPE_COLUMN] = self.type

        db_response = log_duration(
            lambda: self._create_item(content, update_on_exists), "dynamodb_duration_ms"
        )

        status_code = db_response["ResponseMetadata"]["HTTPStatusCode"]
        log_add(dynamodb_status_code=status_code)

        if status_code == 200:
            return item_id
        else:
            msg = f"Error creating item ({status_code}): {db_response}"
            log.exception(msg)
            raise ValueError(msg)

    def _create_item(self, content, update_on_exists):
        """Helper for adding `content` to `self.table`.

        Return the DynamoDB response on success.

        When `update_on_exists` is true, any existing entry will be updated
        with the new content. Otherwise it's required that an entry with the ID
        doesn't already exist.
        """
        try:
            log_add(dynamodb_update_on_exists=update_on_exists)
            if update_on_exists:
                return self.table.put_item(Item=content)
            else:
                cond = "attribute_not_exists(Id) AND attribute_not_exists(#Type)"
                return self.table.put_item(
                    Item=content,
                    ExpressionAttributeNames={"#Type": common.TYPE_COLUMN},
                    ConditionExpression=cond,
                )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ConditionalCheckFailedException":
                item_id = content[common.ID_COLUMN]
                msg = f"Item with id {item_id} already exists"
                log.error(msg)
                raise ResourceConflict(msg, e)
            else:
                msg = e.response["Error"]["Message"]
                log.error(msg)
                raise ValueError(f"Error creating item ({error_code}): {msg}")

    def update_item(self, item_id, content):
        return self._update_item(item_id, content, patch=False)

    def patch_item(self, item_id, content):
        return self._update_item(item_id, content, patch=True)

    def _update_item(self, item_id, content, patch):
        log_add(dynamodb_item_id=item_id, dynamodb_item_type=self.type)
        old_item = self.get_item(item_id)

        item_exists = old_item is not None
        log_add(dynamodb_item_exists=item_exists)
        if not item_exists:
            raise KeyError(f"Item with id {item_id} does not exist")

        new_content = {**old_item, **content} if patch else content

        new_content[common.ID_COLUMN] = old_item[common.ID_COLUMN]
        new_content[common.TYPE_COLUMN] = self.type

        for key in ["accessRights", "confidentiality", "parent_id"]:
            if old_item.get(key) != new_content.get(key):
                raise ValidationError(f"The value of {key} cannot be changed.")

        db_response = log_duration(
            lambda: self.table.put_item(Item=new_content), "dynamodb_duration_ms"
        )

        status_code = db_response["ResponseMetadata"]["HTTPStatusCode"]
        log_add(dynamodb_status_code=status_code)

        if status_code == 200:
            return item_id

        msg = f"Error updating item ({status_code}): {db_response}"
        log.exception(msg)
        raise ValueError(msg)
