# Update last read

This batch job updates the `last_read` metadata fields on all datasets that were
read from S3 on the day before.

To determine the datasets that were read on the day before, the S3 access logs
in the `ok-origo-dataplatform-logs-{dev|prod}` bucket is traversed, looking for
GET requests on keys that look like datasets. The datasets found this way is
then updated with yesterday's date in the `last_read` metadata field.
