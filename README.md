Metadata-api
============

API for posting, updating and retrieving metadata.

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

Requires `saml2aws`


# Concept
The metadata API is structured around the following base concept - the `dataset`:
```
+-- dataset-id=my-dataset
|   +-- version=1
|       +-- edition=20190101T105900
|           +-- distribution=filename.txt
|           +-- distribution=foo.txt
|       +-- edition=20200101T105900
|          +-- distribution=presentation.md
|   +-- version=2
|       +-- edition=20200101T105900
|           +-- distribution=otherfile.md
|       +-- edition=20210101T105900
```
`dataset/version/edition` - `my-dataset/1/20190101T105900`

Each version and edition keeps a version named `latest` (a reserved name for a version and edition), that always contains the latest version/edition POSTed to that resource, and can be accessed with `datasets/my-dataset/versions/latest` to get the latest version and `datasets/my-dataset/version/1/editions/latest`


## API usage
The correct schema definition that is used for validation in the examples below: see `schema/*.json`

### Access
* Create dataset: valid keycloack access token in header: `"Authorization": f"Bearer {accessToken}"`
* Create or update version or edition: valid keycloack access token and owner-access to `:dataset-id`
* List dataset/version/edition: Logged in user

### List all datasets

```
GET /datasets
```
All available datasets. An optional query parameter `parent_id` is accepted for filtering by parent dataset.

### Create dataset

```
POST /datasets

{
    "title": "Besøksdata gjenbruksstasjoner",
    "description": "Sensordata fra tellere på gjenbruksstasjonene",
    "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
    "frequency": "hourly",
    "accessRights": "public",
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
This will create a dataset with ID=besoksdata-gjenbruksstasjoner, the id is derived from the title of the dataset. If another dataset exists with the same ID, a ID will be created with a random set of characters at the end of the id (eg: besoksdata-gjenbruksstasjoner-5C5uX)

### Update dataset

#### Replace

```
PUT /datasets/:dataset-id

{
    "title": "Besøksdata gjenbruksstasjoner oppdatert tittel",
    "description": "Sensordata fra tellere på gjenbruksstasjonene",
    "keywords": ["avfall", "besøkende", "gjenbruksstasjon"],
    "frequency": "hourly",
    "accessRights": "public",
    "privacyLevel": "green",
    "objective": "Formålsbeskrivelse",
    "contactPoint": {
        "name": "Tim",
        "email": "tim@oslo.kommune.no",
        "phone": "11111111"
    },
    "publisher": "REN"
}
```
Updates a single `dataset-id`, replaces old json document

#### Partial update

```
PATCH /datasets/:dataset-id

{
    "title": "Besøksdata gjenbruksstasjoner kun oppdatert tittel"
}
```

Partially updates a single `dataset-id`. **Note** that patching is top-level shallow, i.e. updates inside deep structure values will behave as a PUT.

E.g. `phone` must be supplied in the following PATCH, even though we are just changing `name` and `email`. If `phone` was not supplied, it would be removed.

```
PATCH /datasets/:dataset-id

{
    "contactPoint": {
        "name": "Kim",
        "email": "kim@oslo.kommune.no",
        "phone": "11111111"
    }
}
```

### Get a single dataset

```
GET /datsets/:dataset-id
```

### Create version for a dataset

```
POST /datasets/:dataset-id/versions

{
    "version": "1",
    "schema": {},
    "transformation": {}
}
```
`version` will become  `:version-id` in the examples below

### Update version

```
PUT /datasets/:dataset-id/versions/:version-id

{
    "version": "1",
    "schema": {},
    "transformation": {}
}
```
Updates a single `version-id`, replaces old json document, `version` key must maintain same value as `:version-id`

### Get a version

```
GET /datasets/:dataset-id/versions/:version-id
```

### Get latest version

```
GET /datasets/:dataset-id/versions/latest
```
Get the latest version created on `dataset-id`

### Create new edition

```
POST /datasets/:dataset-id/versions/:version-id/editions

{
    "description": "Data for one hour",
    "startTime": "2018-12-21T08:00:00+01:00", // inclusive
    "endTime": "2018-12-21T09:00:00+01:00"    // exclusive
}
```

### Get edition

```
GET /datasets/:dataset-id/versions/:version-id
```

### Get latest edition

```
GET /datasets/:dataset-id/versions/:version-id/latest
```
Get the latest edition created on `:version-id`

### Create new distribution

```
POST /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions

{
    "filename": "visitors.csv",
    "format": "text/csv",
    "checksum": "..."
}
```
