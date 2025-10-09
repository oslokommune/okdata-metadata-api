import logging
from functools import reduce

from aws_xray_sdk.core import patch_all
from boto3.dynamodb.conditions import And, Attr, Key
from botocore.exceptions import ClientError
from okdata.aws.logging import log_add, log_duration

from metadata.error import (
    DeleteConflict,
    ResourceConflict,
    ResourceNotFoundError,
    ValidationError,
)

patch_all()

log = logging.getLogger()


ID_COLUMN = "Id"
TYPE_COLUMN = "Type"


class MissingParentError(KeyError):
    """Raised when a parent doesn't exist."""

    pass


class CommonRepository:
    def __init__(self, table, type):
        self.table = table
        self.type = type

    def get_item(self, item_id, consistent_read=False):
        log_add(dynamodb_item_id=item_id, dynamodb_item_type=self.type)
        key = {ID_COLUMN: item_id, TYPE_COLUMN: self.type}

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

    def get_items(self, parent_id=None, was_derived_from_name=None):
        log_add(dynamodb_item_type=self.type)
        key_condition = Key(TYPE_COLUMN).eq(self.type)
        filter_conditions = []

        if parent_id:
            log_add(dynamodb_parent_id=parent_id)
            if self.type == "Dataset":
                filter_conditions.append(Key("parent_id").eq(parent_id))
            else:
                key_condition = key_condition & Key(ID_COLUMN).begins_with(
                    f"{parent_id}/"
                )

        if was_derived_from_name:
            log_add(dynamodb_was_derived_from_name=was_derived_from_name)
            filter_conditions.append(
                Attr("wasDerivedFrom.name").eq(was_derived_from_name)
            )

        query_args = {
            "IndexName": "IdByTypeIndex",
            "KeyConditionExpression": key_condition,
        }

        if filter_conditions:
            query_args["FilterExpression"] = reduce(And, filter_conditions)

        db_response = log_duration(
            lambda: self.table.query(**query_args),
            "dynamodb_duration_ms",
        )

        status_code = db_response["ResponseMetadata"]["HTTPStatusCode"]
        log_add(dynamodb_status_code=status_code)

        items = db_response["Items"]
        log_add(dynamodb_num_items=len(items))

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
            parent_key = {ID_COLUMN: parent_id, TYPE_COLUMN: parent_type}
            db_response = self.table.get_item(Key=parent_key)
            parent_exists = "Item" in db_response
            log_add(dynamodb_parent_exists=parent_exists)
            if not parent_exists:
                msg = f"Parent item with id {parent_id} does not exist"
                log.error(msg)
                raise MissingParentError(msg)

        content[ID_COLUMN] = item_id
        content[TYPE_COLUMN] = self.type

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
                    ExpressionAttributeNames={"#Type": TYPE_COLUMN},
                    ConditionExpression=cond,
                )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ConditionalCheckFailedException":
                item_id = content[ID_COLUMN]
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

        new_content[ID_COLUMN] = old_item[ID_COLUMN]
        new_content[TYPE_COLUMN] = self.type

        for key in ["accessRights", "confidentiality", "parent_id"]:
            old_value = old_item.get(key)
            new_value = new_content.get(key)
            # Allow setting a value if it was previously None, but don't allow changing existing values
            if old_value is not None and old_value != new_value:
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

    def delete_item(self, item_id, cascade=False):
        """Delete item with ID `item_id`.

        Delete every child item as well if `cascade` is true, otherwise skip
        deletion and raise `DeleteConflict` if the item has any children.
        """
        log_add(dynamodb_item_id=item_id, dynamodb_item_type=self.type)
        key = {ID_COLUMN: item_id, TYPE_COLUMN: self.type}
        children = self.children(item_id)

        if children:
            if cascade:
                child_repo = self.child_repository()

                for child in children:
                    child_repo.delete_item(child["Id"], True)
            else:
                raise DeleteConflict(f"Item '{item_id}' has children; cannot delete.")

        try:
            log_duration(
                lambda: self.table.delete_item(
                    Key=key, ConditionExpression="attribute_exists(Id)"
                ),
                "dynamodb_duration_ms",
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "ConditionalCheckFailedException":
                msg = f"Item with id '{item_id}' not found"
                log.error(msg)
                raise ResourceNotFoundError(msg)
            else:
                msg = e.response["Error"]["Message"]
                log.error(msg)
                raise ValueError(f"Error deleting item ({error_code}): {msg}")

    def _query_children(self, item_id, child_type):
        return self.table.query(
            IndexName="IdByTypeIndex",
            KeyConditionExpression=Key(TYPE_COLUMN).eq(child_type)
            & Key(ID_COLUMN).begins_with(f"{item_id}/"),
        )["Items"]

    def children(self, item_id):
        raise NotImplementedError

    def child_repository(self):
        raise NotImplementedError
