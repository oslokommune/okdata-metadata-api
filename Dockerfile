FROM public.ecr.aws/lambda/python:3.13

COPY jobs ${LAMBDA_TASK_ROOT}/jobs
COPY metadata ${LAMBDA_TASK_ROOT}/metadata
COPY schema ${LAMBDA_TASK_ROOT}/schema
COPY templates ${LAMBDA_TASK_ROOT}/templates

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

RUN dnf install shadow-utils -y
RUN /sbin/groupadd -r app
RUN /sbin/useradd -r -g app app
RUN chown -R app:app ${LAMBDA_TASK_ROOT}
USER app

CMD ["set-me-in-serverless.yaml"]
