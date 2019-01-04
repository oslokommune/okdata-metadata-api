data "template_file" "metadata-api-spec" {
  template = "${file("metadata-api.yaml")}"

  vars {
    list_datasets_lambda_uri = "${aws_lambda_function.list_datasets.invoke_arn}"
    create_dataset_lambda_uri = "${aws_lambda_function.create_dataset.invoke_arn}"
    get_dataset_lambda_uri = "${aws_lambda_function.get_dataset.invoke_arn}"
    lambda_role = "${aws_iam_role.metadata_api_exec.arn}"
  }
}

resource "aws_lambda_function" "list_datasets" {
  function_name = "list_datasets"
  filename = "lambda.zip"

  handler = "main.list_datasets"
  runtime = "python3.7"

  role = "${aws_iam_role.metadata_api_exec.arn}"
}

resource "aws_lambda_function" "create_dataset" {
  function_name = "create_dataset"
  filename = "lambda.zip"

  handler = "main.create_dataset"
  runtime = "python3.7"

  role = "${aws_iam_role.metadata_api_exec.arn}"
}

resource "aws_lambda_function" "get_dataset" {
  function_name = "get_dataset"
  filename = "lambda.zip"

  handler = "main.get_dataset"
  runtime = "python3.7"

  role = "${aws_iam_role.metadata_api_exec.arn}"
}

resource "aws_iam_role" "metadata_api_exec" {
  name = "MetadataAPILambdaRole"

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

  source_arn = "${aws_api_gateway_deployment.MetadataAPIDeployment.execution_arn}/GET/datasets"
}

resource "aws_lambda_permission" "create_dataset" {
  statement_id = "AllowAPIGatewayInvoke"
  action = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.create_dataset.arn}"
  principal = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_deployment.MetadataAPIDeployment.execution_arn}/POST/datasets"
}

resource "aws_lambda_permission" "get_dataset" {
  statement_id = "AllowAPIGatewayInvoke"
  action = "lambda:InvokeFunction"
  function_name = "${aws_lambda_function.get_dataset.arn}"
  principal = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_deployment.MetadataAPIDeployment.execution_arn}/GET/datasets/*"
}

output "base_url" {
  value = "${aws_api_gateway_deployment.MetadataAPIDeployment.invoke_url}"
}
