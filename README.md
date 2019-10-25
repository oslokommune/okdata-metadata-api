Metadata-api
============

API for posting, updating and retrieving metadata.

Feel free to add any fields you'd like/need.

## Setup

1. Install [Serverless Framework](https://serverless.com/framework/docs/getting-started/)
2. Setup venv
```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
3. Install Serverless plugins: `make init`
4. Install Python toolchain: `python3 -m pip install (--user) tox black pip-tools`
   - If running with `--user` flag, add `$HOME/.local/bin` to `$PATH`


## Input Validation
The input is validated with json schema, see the models under `schema/`


## Formatting code

Code is formatted using [black](https://pypi.org/project/black/): `make format`

## Running tests

Tests are run using [tox](https://pypi.org/project/tox/): `make test`

For tests and linting we use [pytest](https://pypi.org/project/pytest/), [flake8](https://pypi.org/project/flake8/) and [black](https://pypi.org/project/black/).

## Deploy

`make deploy` or `make deploy-prod`

## API usage

### Register dataset

```
POST /datasets

{
    "title": "Antall besøkende på gjenbruksstasjoner",
    "description": "Sensordata fra tellere på gjenbruksstasjonene",
    "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
    "frequency": "hourly",
    "accessRights": ":non-public",
    "privacyLevel": "green",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "98765432"
    },
    "publisher": "REN"
}
```

### Update dataset


```
PUT datasets/:dataset-id
```

### Create a new version

```
POST /datasets/:dataset-id/versions

{
    "version": "1",
    "schema": {},
    "transformation": {}
}
```

### Create new edition

```
POST /datasets/:dataset-id/versions/:version-id/editions

{
    "description": "Data for one hour",
    "startTime": "2018-12-21T08:00:00+01:00", // inclusive
    "endTime": "2018-12-21T09:00:00+01:00"    // exclusive
}
```

### Create new distribution

```
POST /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions

{
    "filename": "visitors.csv",
    "format": "text/csv",
    "checksum": "..."
}
```
