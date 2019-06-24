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
    url="https://github.oslo.kommune.no/origo-dataplatform/metadata-api",
    py_modules=[
        "common",
        "dataset_handler",
        "version_handler",
        "edition_handler",
        "distribution_handler",
        "CommonRepository",
        "dataset_repository",
        "version_repository",
        "edition_repository",
        "distribution_repository",
    ],
    install_requires=["simplejson==3.16.0", "shortuuid==0.5.0", "aws_xray_sdk>=2.4.2"],
)
