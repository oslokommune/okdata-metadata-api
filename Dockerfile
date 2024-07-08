FROM public.ecr.aws/lambda/python:3.12

COPY metadata ${LAMBDA_TASK_ROOT}/metadata
COPY schema ${LAMBDA_TASK_ROOT}/schema
COPY templates ${LAMBDA_TASK_ROOT}/templates

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

CMD ["set-me-in-serverless.yaml"]
