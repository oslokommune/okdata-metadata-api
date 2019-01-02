provider "aws" {
  region = "eu-west-1"
  version = "~> 1.54"
}

/* Set up remote S3 backend for Terraform state */
terraform {
  backend "s3" {
    bucket = "ok-origo-dataplatform-config-dev"
    key = "terraform/metadata-api-dev/terraform.tfstate"
    region = "eu-central-1"
    dynamodb_table = "terraform-state-lock-dataplatform-dev"
  }
}
