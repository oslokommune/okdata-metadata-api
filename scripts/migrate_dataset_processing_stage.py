import boto3
import requests
from boto3.dynamodb.conditions import Attr, Key


class MigrateDatasetProcessingStage:
    def __init__(self, env, dry_run):
        self.env = env
        self.dry_run = dry_run
        self.bucket = f"ok-origo-dataplatform-{env}"
        self.metadata_api_url = "https://api.data{}.oslo.systems/metadata/".format(
            "" if env == "prod" else "-dev"
        )
        self.s3 = boto3.client("s3")
        self.dynamodb = boto3.resource("dynamodb", "eu-west-1")
        self.metadata_table = self.dynamodb.Table("dataset-metadata")
        self.pipeline_table = self.dynamodb.Table("pipeline")

    def migrate(self, dataset, dest_stage, delete):
        dataset_uri = f"{self.metadata_api_url}{self.env}/datasets/{dataset}"
        print(f"Fetching dataset '{dataset}' metadata from {dataset_uri}")
        resp = requests.get(dataset_uri)
        if resp.status_code != 200:
            raise ValueError(
                f"Unable to fetch metadata for dataset '{dataset}': {resp}"
            )

        dataset_metadata = resp.json()
        confidentiality = dataset_metadata["confidentiality"]
        src_stage = dataset_metadata["processing_stage"]
        if src_stage == dest_stage:
            print(
                f"Dataset '{dataset}' already migrated to stage '{dest_stage}', exiting."
            )
            return

        print(
            f"Migrating dataset '{dataset}' from stage '{src_stage}' to '{dest_stage}'"
        )
        print()

        self.move_s3_files(dataset, confidentiality, src_stage, dest_stage, delete)
        print()
        self.update_output_stage(dataset, dest_stage)
        print()
        # self.update_pipeline_inputs(dataset, src_stage, dest_stage)
        # print()
        print("Done")

    def move_s3_files(
        self, dataset, confidentiality, src_stage, dest_stage, delete_src
    ):
        print(f"Moving S3 files from stage '{src_stage}' to '{dest_stage}'")

        prefix = f"{src_stage}/{confidentiality}/{dataset}/"
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

        # status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if "Contents" not in response:
            print(f"No files found in S3 ({prefix})")
            return

        keys = [o["Key"] for o in response["Contents"]]

        for key in keys:
            dest_key = dest_stage + key[key.index("/") :]
            self.copy_file(key, dest_key)

        if delete_src:
            for key in keys:
                self.delete_file(key)

    def copy_file(self, src, dest):
        print(f"Copying {src} to {dest}")
        if not self.dry_run:
            copy_source = {"Bucket": self.bucket, "Key": src}
            response = self.s3.copy_object(
                Bucket=self.bucket, CopySource=copy_source, Key=dest
            )
            status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            if status_code != 200 or "Error" in response:
                print(f"Error copying file: {response}")
                exit(1)

    def delete_file(self, key):
        print(f"Deleting {key}")
        if not self.dry_run:
            response = self.s3.delete_object(Bucket=self.bucket, Key=key)
            status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            if status_code != 204 or "Error" in response:
                print(f"Error deleting file: {response}")
                exit(1)

    def update_output_stage(self, dataset, stage):
        print(f"Update dataset '{dataset}' output stage to '{stage}'")
        if not self.dry_run:
            self.metadata_table.update_item(
                Key={"Type": "Dataset", "Id": dataset},
                UpdateExpression="SET processing_stage = :ps",
                ExpressionAttributeValues={":ps": stage},
            )

    def update_pipeline_inputs(self, dataset, src_stage, dest_stage):
        print(
            f"Update pipeline inputs for dataset '{dataset}' from stage '{src_stage}' to '{dest_stage}'"
        )
        # TODO handle version > 1
        sort_key = f"input/{dataset}/1"

        response = self.pipeline_table.query(
            IndexName="GSI-1",
            KeyConditionExpression=Key("SK(GSI-1-PK)").eq(sort_key),
            FilterExpression=Attr("stage").eq(src_stage),
        )

        for item in response["Items"]:
            pk = item["PK"]
            print(f"Updating pipeline {pk}")

            if not self.dry_run:
                self.pipeline_table.update_item(
                    Key={"PK": pk, "SK(GSI-1-PK)": sort_key},
                    UpdateExpression="SET stage = :s",
                    ExpressionAttributeValues={":s": dest_stage},
                )
