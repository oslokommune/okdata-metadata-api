# Upload experiment

Prototype for data upload API / service

## Running the Terraform script

Make sure to create a new deployable zip-file with the lambda code first before deploying: `zip lambda.zip main.py`

Deploy the functions as normal using `terraform apply`

## Register new dataset and upload new distribution flow

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

Questions:
* Should we ask the user to categorise the datasets directly, i.e. according to https://publications.europa.eu/en/web/eu-vocabularies/at-concept-scheme/-/resource/authority/data-theme/?target=Browse and http://psi.norge.no/los/struktur.html ? Or better to automatically derive category/theme from title, description and keywords, as recommended here https://guidance.data.gov.uk/theme.html ?
* How to specify contact point, publisher? Use JSON versions of https://www.w3.org/TR/vcard-rdf/ and http://xmlns.com/foaf/spec/#term_Agent ?

Allowed frequencies:
* Manual / irregular
* Periodic (frequency = yearly, monthly, weekly, daily, hourly)
* Continuous
* https://publications.europa.eu/en/web/eu-vocabularies/at-concept-scheme/-/resource/authority/frequency/?target=Browse

### Create a new version

```
POST /datasets/:dataset-id/versions

{
    "version": "1", // optional, defaults to '1'?
    "schema": {}, // optional schema, e.g. inline JSON schema?
    "transformation": {} // optional cleanup transformation, e.g. JSLT?
}
```

Questions:
* Should versions be explicit in URL? Or another (related) dataset id?
* Upload schemas and transformation separately? How to handle changes/fixes in transformations?

### Create new edition

```
POST /datasets/:dataset-id/versions/:version-id/editions

{
    "description": "Data for one hour",
    "startTime": "2018-12-21T08:00:00+01:00", // inclusive
    "endTime": "2018-12-21T09:00:00+01:00"    // exclusive
}
```

Returns an edition with current timestamp (in UTC) as ID, e.g. `20181221T081523` (and sequence number / random id?).

Alternatively, allow creating a new edition with a known ID directly? E.g. for a known timestamp we can use

`POST /datasets/:dataset-id/versions/:version-id/editions/20181221T070000` (or `PUT`?)

### Create new distribution

```
POST /datasets/:dataset-id/versions/:version-id/editions/:edition-id/distributions

{
    "filename": "visitors.csv",
    "format": "text/csv",
    "checksum": "..." // optional MD5 checksum
}
```

which returns a payload containing a signed S3 URL to be used for uploading the actual content.

For a periodic/continuous dataset, data will be stored in S3 at (skipping hour, day, month levels if not applicable):

`/incoming/:privacy-level/:dataset-id/version=:version-id/year=:year/month=:month/day=:day/hour=:hour/:filename`

For manual/irregular dataset, data will be stored in S3 at:

`/incoming/:privacy-level/:dataset-id/version=:version-id/:edition-id/:filename`
