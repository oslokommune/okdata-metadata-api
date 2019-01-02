resource "aws_api_gateway_rest_api" "MetadataAPI" {
  name = "MetedataAPI"
  description = "Metadata API for the data platform"
  body = "${file("metadata-api.yaml")}"
}