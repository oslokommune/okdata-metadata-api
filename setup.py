import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="metadata-api",
    version="0.0.1",
    author="Origo Dataplattform",
    author_email="dataplattform@oslo.kommune.no",
    description="Api for managing metadata catalogue",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/oslokommune/okdata-metadata-api/",
    py_modules=[
        "metadata.common",
        "metadata.dataset.handler",
        "metadata.version.handler",
        "metadata.edition.handler",
        "metadata.distribution.handler",
        "metadata.CommonRepository",
        "metadata.dataset.repository",
        "metadata.version.repository",
        "metadata.edition.repository",
        "metadata.distribution.repository",
    ],
    install_requires=[
        "boto3",
        "black",
        "isort",
        "jinja2",
        "markupsafe",
        "aws-xray-sdk",
        "requests",
        "simplejson",
        "jsonschema[format]",
        "strict-rfc3339",
        "okdata-aws>=4.1",
        "python-keycloak",
        "okdata-resource-auth",
    ],
    python_requires="==3.12.*",
)
