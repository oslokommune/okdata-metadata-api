data "template_file" "metadata-api-spec" {
  template = "${file("metadata-api.yaml")}"

  vars {
    lambda_uri = "${aws_lambda_function.list_datasets.invoke_arn}"
    lambda_role = "${aws_iam_role.list_datasets_exec.arn}"
  }
}

resource "aws_lambda_function" "list_datasets" {
  function_name = "list_datasets"
  filename = "hello.zip"

  handler = "main.hello"
  runtime = "python3.6"

  role = "${aws_iam_role.list_datasets_exec.arn}"
}

resource "aws_iam_role" "list_datasets_exec" {
  name = "listDatasetsLambdaRole"

  assume_role_policy = "${file("lambda_assume_role_policy.json")}"
}

resource "aws_api_gateway_rest_api" "MetadataAPI" {
  name = "MetadataAPI"
  description = "Metadata API for the data platform"
  body = "${data.template_file.metadata-api-spec.rendered}"
}

resource "aws_api_gateway_deployment" "MetadataAPIDeployment" {
  rest_api_id = "${aws_api_gateway_rest_api.MetadataAPI.id}"
  stage_name = "v1"
}

resource "aws_lambda_permission" "list_datasets" {
  statement_id = "AllowAPIGatewayInvoke"
  action = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.list_datasets.arn}"
  principal = "apigateway.amazonaws.com"

  source_arn = "${replace(aws_api_gateway_deployment.MetadataAPIDeployment.execution_arn, "/v1", "/*")}/GET/datasets"
}

output "base_url" {
  value = "${aws_api_gateway_deployment.MetadataAPIDeployment.invoke_url}"
}
