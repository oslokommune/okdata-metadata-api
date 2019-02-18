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
    packages=setuptools.find_packages(),
    install_requires=[
        'simplejson==3.16.0'
    ]
)